import os
import pandas as pd
import time
import json
from dotenv import load_dotenv
from datetime import datetime
from dateutil.relativedelta import relativedelta
from connectors.google_sheet import connect_gsheet
from connectors.semrush import *

load_dotenv()
# Charger la variable d'environnement"
service_account_info = json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))
# ------------------------------
# CONFIGURATION
# ------------------------------
API_KEY = "f48f8801557c385665692dced469e22e"  # <-- remplace par ta clé
DATABASE = "fr"
GOOGLE_SHEET_ID = os.environ['GOOGLE_SHEET_ID']
SOURCE_SHEET_NAME = "Team Data- URLS"
TARGET_SHEET_NAME = "monthly_run"
HISTORIC_MONTHS = 2  # nombre de mois à récupérer pour l’historique
#json_account = os.environ['credentials_service_account']
# ------------------------------
# FONCTIONS
# ------------------------------
def get_monthly_dates(months_back: int):
    """Retourne une liste de dates correspondant au 15 de chaque mois, incluant le mois courant."""
    today = datetime.today()
    dates = []
    for i in range(months_back, 0, -1):  # inclut le mois courant
        month_date = today - relativedelta(months=i)
        month_date = month_date.replace(day=15)
        dates.append(month_date)
    return dates

# ------------------------------
# SCRIPT PRINCIPAL
# ------------------------------
def main():
    print("🔗 Connexion à Google Sheets…")
    source_sheet = connect_gsheet(GOOGLE_SHEET_ID, SOURCE_SHEET_NAME,service_account_info)
    target_sheet = connect_gsheet(GOOGLE_SHEET_ID, TARGET_SHEET_NAME,service_account_info)

    print(f"📥 Lecture de la feuille source '{SOURCE_SHEET_NAME}'…")
    source_data = source_sheet.get_all_records(expected_headers=None)
    df_source = pd.DataFrame(source_data)
    df_source = df_source[["URLs optimisées et publiées", "Catégorie", "MC principal optimise", "Date Intégration"]]

    print(f"📥 Lecture de la feuille cible '{TARGET_SHEET_NAME}' pour éviter les doublons…")
    target_data = target_sheet.get_all_records(expected_headers=None)
    if target_data:
        df_target = pd.DataFrame(target_data)
    else:
        df_target = pd.DataFrame(columns=["URL", "Mot-clé", "Mois", "Position SEMrush"])

    monthly_dates = get_monthly_dates(HISTORIC_MONTHS)
    new_rows = []
    df_source=df_source[:5]
    for _, row in df_source.iterrows():
        url = row["URLs optimisées et publiées"].strip()
        keyword = row["MC principal optimise"].strip()

        for month_date in monthly_dates:
            month_str = month_date.strftime("%Y-%m")
            
            # Vérifie si la combinaison existe déjà
            exists = ((df_target["URL"] == url) &
                      (df_target["Mot-clé"] == keyword) &
                      (df_target["Mois"] == month_str)).any()
            if exists:
                print(f"ℹ️ Ligne existante pour {url} / {keyword} / {month_str}, sautée")
                continue

            display_date = format_semrush_date(month_date)
            print(f"\n🔹 URL : {url}\n   Mot-clé : {keyword}\n   Date : {display_date}")

            position = get_position_semrush(API_KEY, DATABASE, keyword, url, display_date)
            new_rows.append([url, keyword, month_str, position])
            time.sleep(1)

    # -------------------
    # Écriture dans la feuille 'monthly_run'
    # -------------------
    if new_rows:
        existing_cols = target_sheet.row_values(1)
        headers = ["URL", "Mot-clé", "Mois", "Position SEMrush"]

        # Ajout des colonnes si elles n’existent pas
        missing_headers = [h for h in headers if h not in existing_cols]
        if missing_headers:
            print(f"🆕 Ajout des colonnes : {', '.join(missing_headers)}")
            start_col_index = len(existing_cols) + 1
            for i, h in enumerate(missing_headers):
                target_sheet.update_cell(1, start_col_index + i, h)
            existing_cols += missing_headers

        print("📝 Écriture des nouvelles lignes dans Google Sheets…")
        start_row = len(target_sheet.get_all_values()) + 1
        for i, row_vals in enumerate(new_rows):
            target_sheet.update(f"A{start_row + i}", [row_vals])

    print("\n✅ Mise à jour terminée dans la feuille 'monthly_run'.")

# ------------------------------
if __name__ == "__main__":
    main()
