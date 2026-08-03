import os
import json

from google.oauth2.service_account import Credentials
import gspread


def load_service_account_info():
    """Charge le JSON du compte de service depuis GOOGLE_CREDENTIALS_JSON.

    Tolère les valeurs issues d'un copier-coller du .env : espaces autour,
    guillemets simples ou doubles englobants. Lève une erreur explicite si la
    variable est absente ou vide (au lieu du cryptique JSONDecodeError char 0).
    """
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON")
    if raw is None or not raw.strip():
        raise RuntimeError(
            "La variable d'environnement GOOGLE_CREDENTIALS_JSON est absente ou vide. "
            "Vérifie le secret monté sur le job Cloud Run (nom de la variable et version du secret)."
        )
    raw = raw.strip()
    # Retire d'éventuels guillemets englobants ('...' ou \"...\").
    if len(raw) >= 2 and raw[0] == raw[-1] and raw[0] in ("'", '"'):
        raw = raw[1:-1].strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(
            "GOOGLE_CREDENTIALS_JSON n'est pas un JSON valide. La valeur du secret doit être "
            "le contenu brut du fichier de compte de service (commençant par '{' et finissant "
            f"par '}}'), sans guillemets ni préfixe. Détail : {exc}"
        ) from exc


def connect_gsheet(sheet_id, sheet_name,json_credentials):
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_info(json_credentials, scopes=scopes)
    client = gspread.authorize(creds)
    sheet = client.open_by_key(sheet_id).worksheet(sheet_name)
    return sheet