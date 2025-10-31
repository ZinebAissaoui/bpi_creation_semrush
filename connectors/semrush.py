
import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
import os
DEBUG_FOLDER = "debug_semrush"     # dossier pour stocker les réponses brutes

os.makedirs(DEBUG_FOLDER, exist_ok=True)

# ------------------------------
# FONCTIONS UTILES
# ------------------------------
def to_date(date_str):
    """Convertit une date JJ/MM/AAAA en datetime."""
    return datetime.strptime(date_str, "%d/%m/%Y")

def format_semrush_date(dt):
    """Formate une date au format SEMrush YYYYMM15."""
    return dt.strftime("%Y%m") + "15"

def save_debug_response(keyword, display_date, content):
    """Sauvegarde la réponse brute SEMrush pour debug."""
    safe_kw = keyword.replace(" ", "_").replace("/", "_")
    file_path = os.path.join(DEBUG_FOLDER, f"semrush_{safe_kw}_{display_date}.csv")
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(content)

def get_position_semrush(api_key, database, keyword, target_url, display_date=None):
    """Récupère la position d'une URL sur un mot-clé via SEMrush.
       Si le fichier debug existe déjà, il l'utilise directement."""
    
    # Nom du fichier debug
    safe_kw = keyword.replace(" ", "_").replace("/", "_")
    debug_file = os.path.join(DEBUG_FOLDER, f"semrush_{safe_kw}_{display_date or 'latest'}.csv")
    
    # ⚡ Si le fichier debug existe déjà, on l'utilise
    if os.path.exists(debug_file):
        with open(debug_file, "r", encoding="utf-8") as f:
            lines = f.read().strip().split("\n")
    else:
        # Sinon, on fait la requête SEMrush
        endpoint = "https://api.semrush.com/"
        params = {
            "type": "phrase_organic",
            "key": api_key,
            "phrase": keyword,
            "database": database,
            "export_columns": "Po,Ur",
            "display_limit": 100,
        }
        if display_date:
            params["display_date"] = display_date

        try:
            r = requests.get(endpoint, params=params)
            if r.status_code != 200:
                print(f"⚠️ Erreur {r.status_code} pour '{keyword}' ({display_date})")
                if display_date:
                    return get_position_semrush(api_key, database, keyword, target_url, display_date=None)
                return None

            lines = r.text.strip().split("\n")
            # Sauvegarde pour debug
            os.makedirs(DEBUG_FOLDER, exist_ok=True)
            with open(debug_file, "w", encoding="utf-8") as f:
                f.write(r.text)

        except Exception as e:
            print(f"⚠️ Erreur de requête SEMrush ({keyword}, {display_date}): {e}")
            return None

    # Parse les lignes
    if len(lines) <= 1:
        return None

    for line in lines[1:]:
        parts = line.split(";")
        if len(parts) >= 2:
            pos, url = parts[0], parts[1]
            if target_url.lower() in url.lower():
                return float(pos)
    return None
