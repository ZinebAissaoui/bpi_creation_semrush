import pandas as pd
import time
from dotenv import load_dotenv
import json
from datetime import datetime
from dateutil.relativedelta import relativedelta
from connectors.google_sheet import connect_gsheet
from connectors.semrush_local import *
load_dotenv()

# ------------------------------
# CONFIGURATION
# ------------------------------
API_KEY =  os.environ['api_key_semrush'] # <-- remplace par ta clé
DATABASE = "fr"                   # exemple : fr, us, uk, es, etc.
#INPUT_CSV = "urls_semrush.csv"
#OUTPUT_CSV = "urls_semrush_updated.csv"
GOOGLE_SHEET_ID = os.environ['GOOGLE_SHEET_ID']  # <-- remplace par l’ID de ton Google Sheet
service_account_info = json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))

SHEET_NAME = "Team Data- URLS"
# ------------------------------
# SCRIPT PRINCIPAL
# ------------------------------
def main():
    print("🔗 Connexion à Google Sheets…")
    sheet = connect_gsheet(GOOGLE_SHEET_ID, SHEET_NAME,service_account_info)

    print(f"📥 Lecture de la feuille '{SHEET_NAME}'…")
    # ⚙️ Ignore les colonnes en double avec expected_headers
    data = sheet.get_all_records(expected_headers=None)
    df = pd.DataFrame(data)

    print(f"✅ {len(df)} lignes trouvées")

    # On ne garde que les colonnes utiles
    df = df[["URLs optimisées et publiées", "Catégorie", "MC principal optimise", "Date Intégration"]]

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
    for row_idx, vals in enumerate(new_positions, start=2):
        range_str = f"{chr(64 + col_indexes[0])}{row_idx}:{chr(64 + col_indexes[-1])}{row_idx}"
        sheet.update(values=[vals], range_name=range_str)

    print("\n✅ Mise à jour terminée dans la feuille Google Sheets.")


# ------------------------------
if __name__ == "__main__":
    main()