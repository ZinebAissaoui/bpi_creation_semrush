import pandas as pd
import time
import argparse
from dotenv import load_dotenv
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from gspread.utils import rowcol_to_a1
from connectors.google_sheet import connect_gsheet, load_service_account_info
# Cache SEMrush persistant dans Google Cloud Storage (utile en conteneur éphémère).
from connectors.semrush import *
load_dotenv()

# ------------------------------
# CONFIGURATION
# ------------------------------
API_KEY =  os.environ['api_key_semrush'] # <-- remplace par ta clé
DATABASE = "fr"                   # exemple : fr, us, uk, es, etc.
#INPUT_CSV = "urls_semrush.csv"
#OUTPUT_CSV = "urls_semrush_updated.csv"
GOOGLE_SHEET_ID = os.environ['GOOGLE_SHEET_ID']  # <-- remplace par l’ID de ton Google Sheet
service_account_info = load_service_account_info()

SHEET_NAME = "Team Data- URLS"
# ------------------------------
# SCRIPT PRINCIPAL
# ------------------------------
def main(start_row=2):
    # start_row = numéro de ligne du Sheet où commencer (1 = en-têtes, 2 = 1re ligne de données)
    if start_row < 2:
        raise ValueError("start_row doit être >= 2 (la ligne 1 contient les en-têtes)")

    print("🔗 Connexion à Google Sheets…")
    sheet = connect_gsheet(GOOGLE_SHEET_ID, SHEET_NAME,service_account_info)

    print(f"📥 Lecture de la feuille '{SHEET_NAME}'…")
    # ⚙️ Ignore les colonnes en double avec expected_headers
    data = sheet.get_all_records(expected_headers=None)
    df = pd.DataFrame(data)

    print(f"✅ {len(df)} lignes trouvées au total")

    # On ne garde que les colonnes utiles
    df = df[["URLs optimisées et publiées", "Catégorie", "MC principal optimise", "Date Intégration"]]

    # 🔁 Reprise incrémentale : on ne traite qu'à partir de start_row.
    # df index 0 correspond à la ligne 2 du Sheet -> décalage de 2.
    df_start_index = start_row - 2
    df = df.iloc[df_start_index:]
    print(f"▶️ Reprise à la ligne {start_row} du Sheet -> {len(df)} lignes à traiter")

    new_positions = []

    for _, row in df.iterrows():
        url = row["URLs optimisées et publiées"].strip()
        keyword = row["MC principal optimise"].strip()
        date_integration = to_date(row["Date Intégration"])

        date_m1 = date_integration - relativedelta(months=1)
        date_m1plus = date_integration + relativedelta(months=1)
        display_date_m1 = format_semrush_date(date_m1)
        display_date_m1plus = format_semrush_date(date_m1plus)

        print(f"\n🔹 URL : {url}\n   Mot-clé : {keyword}")
        print(f"   Dates : M-1={display_date_m1}, M+1={display_date_m1plus}")

        pos_m1 = get_position_semrush(API_KEY, DATABASE, keyword, url, display_date_m1)
        pos_m1plus = get_position_semrush(API_KEY, DATABASE, keyword, url, display_date_m1plus)

        gain = None
        if pos_m1 is not None and pos_m1plus is not None:
            gain = pos_m1 - pos_m1plus

        new_positions.append([pos_m1, pos_m1plus, gain])
        time.sleep(1)

    # -------------------
    # Ajout des colonnes SEMrush sans doublons
    # -------------------
    existing_cols = sheet.row_values(1)
    headers = ["Semrush - Position M-1", "Semrush - Position M+1", "Semrush - Gain en position"]

    # Vérifie si les colonnes existent déjà
    missing_headers = [h for h in headers if h not in existing_cols]
    if missing_headers:
        print(f"🆕 Ajout des nouvelles colonnes : {', '.join(missing_headers)}")
        start_col_index = len(existing_cols) + 1
        for i, h in enumerate(missing_headers):
            sheet.update_cell(1, start_col_index + i, h)
        existing_cols += missing_headers
    else:
        print("ℹ️ Les colonnes SEMrush existent déjà, mise à jour des valeurs uniquement.")

    # Index exact des colonnes à remplir
    col_indexes = [existing_cols.index(h) + 1 for h in headers]

    print("📝 Écriture des résultats dans Google Sheets…")
    # On écrit à partir de start_row (et non plus systématiquement à la ligne 2).
    for row_idx, vals in enumerate(new_positions, start=start_row):
        start_cell = rowcol_to_a1(row_idx, col_indexes[0])
        end_cell = rowcol_to_a1(row_idx, col_indexes[-1])
        range_str = f"{start_cell}:{end_cell}"
        sheet.update(values=[vals], range_name=range_str)

    print("\n✅ Mise à jour terminée dans la feuille Google Sheets.")


# ------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remplit les colonnes SEMrush du Google Sheet.")
    parser.add_argument(
        "--start-row",
        type=int,
        default=2,
        help="Numéro de ligne du Sheet où commencer (1 = en-têtes, 2 = 1re ligne de données). Défaut : 2 (tout le fichier).",
    )
    cli_args = parser.parse_args()
    main(start_row=cli_args.start_row)