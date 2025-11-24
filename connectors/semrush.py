from google.cloud import storage
import json
import os

import pandas as pd
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dotenv import load_dotenv
load_dotenv()
DEBUG_FOLDER = "debug_semrush"     # dossier pour stocker les réponses brutes

os.makedirs(DEBUG_FOLDER, exist_ok=True)

# Authentification (même JSON que pour Google Sheets)
service_account_info =  json.loads(os.getenv("GOOGLE_CREDENTIALS_JSON"))


storage_client = storage.Client.from_service_account_info(service_account_info)
bucket_name = os.getenv("GCP_BUCKET_NAME")
bucket = storage_client.bucket(bucket_name)

def gcs_blob_exists(blob_name):
    blob = bucket.blob(blob_name)
    return blob.exists()

def gcs_read_text(blob_name):
    blob = bucket.blob(blob_name)
    return blob.download_as_text()

def gcs_write_text(blob_name, text):
    blob = bucket.blob(blob_name)
    blob.upload_from_string(text, content_type="text/plain")

def to_date(date_str):
    """Convertit une date JJ/MM/AAAA en datetime."""
    return datetime.strptime(date_str, "%d/%m/%Y")

def format_semrush_date(dt):
    """Formate une date au format SEMrush YYYYMM15."""
    return dt.strftime("%Y%m") + "15"

def save_debug_response(keyword, display_date, content):
    safe_kw = keyword.replace(" ", "_").replace("/", "_")
    blob_name = f"semrush_{safe_kw}_{display_date}.csv"
    gcs_write_text(blob_name, content)

def get_position_semrush(api_key, database, keyword, target_url, display_date=None):
    safe_kw = keyword.replace(" ", "_").replace("/", "_")
    blob_name = f"semrush_{safe_kw}_{display_date or 'latest'}.csv"

    # ⚡ Si déjà dans GCS → on lit directement
    if gcs_blob_exists(blob_name):
        content = gcs_read_text(blob_name)
        lines = content.strip().split("\n")
    else:
        # 🔥 Sinon → on appelle SEMrush
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

        r = requests.get(endpoint, params=params)
        if r.status_code != 200:
            print(f"⚠️ Erreur {r.status_code} pour {keyword}")
            return None

        content = r.text
        lines = content.strip().split("\n")

        # 💾 Sauvegarde dans Google Cloud Storage
        gcs_write_text(blob_name, content)

    if len(lines) <= 1:
        return None

    for line in lines[1:]:
        parts = line.split(";")
        if len(parts) >= 2:
            pos, url = parts[0], parts[1]
            if target_url.lower() in url.lower():
                return float(pos)

    return None
