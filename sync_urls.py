"""
Synchronisation des URLs : "Evolution urls optimisées" -> "Team Data- URLS".

Copie dans "Team Data- URLS" les URLs présentes dans "Evolution urls optimisées"
qui remplissent TOUTES les conditions suivantes :
  - mois d'intégration STRICTEMENT antérieur à (mois actuel - 2)
    (autrement dit intégration <= mois actuel - 3), afin que le mois M+1 de la
    date d'intégration soit clôturé chez SEMrush avant le calcul des positions ;
  - le couple (URL, mot-clé) n'existe pas déjà dans "Team Data- URLS".

La fonction publique `sync_new_urls()` renvoie le numéro de la 1re ligne ajoutée
dans "Team Data- URLS" (pour piloter main.py), ou None si rien n'a été ajouté.
"""
import os
import json
import argparse
import unicodedata
from datetime import datetime

import pandas as pd
from dotenv import load_dotenv
from dateutil.relativedelta import relativedelta

from connectors.google_sheet import connect_gsheet, load_service_account_info

load_dotenv()
service_account_info = load_service_account_info()

# ------------------------------
# CONFIGURATION
# ------------------------------
GOOGLE_SHEET_ID = os.environ["GOOGLE_SHEET_ID"]
SOURCE_SHEET_NAME = "Evolution urls optimisées"
TARGET_SHEET_NAME = "Team Data- URLS"

# Nombre de mois de "sécurité" : on n'intègre que les URLs dont le mois
# d'intégration est < (mois actuel - MONTHS_MARGIN).
# Avec 1 : en mois X, on intègre jusqu'à M-2 (dont le M+1 = M-1, mois déjà
# clôturé). Passer à 2 pour une marge supplémentaire si les données SEMrush
# du mois précédent ne sont pas encore stabilisées au moment du run.
MONTHS_MARGIN = 1

# Correspondance mois français (abréviations Excel/Sheets locale FR) -> numéro.
# Les clés sont normalisées (sans accent, sans point final, en minuscules).
FRENCH_MONTHS = {
    "janv": 1,
    "fevr": 2,
    "mars": 3,
    "avr": 4,
    "mai": 5,
    "juin": 6,
    "juil": 7,
    "aout": 8,
    "sept": 9,
    "oct": 10,
    "nov": 11,
    "dec": 12,
}


# ------------------------------
# UTILITAIRES
# ------------------------------
def _strip_accents(text: str) -> str:
    """Retire les accents et met en minuscules."""
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(c for c in normalized if not unicodedata.combining(c)).lower()


def parse_french_month(value: str):
    """Convertit une valeur type 'août-24' / 'sept.-24' / 'avr.-26' en (année, mois).

    Renvoie None si la valeur n'est pas interprétable.
    """
    if not value:
        return None
    raw = value.strip()
    if "-" not in raw:
        return None
    month_part, year_part = raw.rsplit("-", 1)
    key = _strip_accents(month_part).rstrip(".").strip()
    month = FRENCH_MONTHS.get(key)
    if month is None:
        return None
    year_digits = "".join(ch for ch in year_part if ch.isdigit())
    if not year_digits:
        return None
    year = int(year_digits)
    if year < 100:  # année sur 2 chiffres -> 20xx
        year += 2000
    return year, month


def _month_index(year: int, month: int) -> int:
    """Index absolu du mois pour comparer facilement deux (année, mois)."""
    return year * 12 + (month - 1)


def _find_col(header, *needles):
    """Renvoie l'index de la 1re colonne dont l'en-tête (normalisé) contient
    tous les fragments donnés. Renvoie None si introuvable."""
    for idx, name in enumerate(header):
        norm = _strip_accents(name)
        if all(_strip_accents(n) in norm for n in needles):
            return idx
    return None


# ------------------------------
# SYNCHRONISATION
# ------------------------------
def sync_new_urls(dry_run: bool = False):
    """Copie les nouvelles URLs éligibles de la feuille Evolution vers Team Data.

    Args:
        dry_run: si True, n'écrit rien et se contente d'afficher les candidats.

    Returns:
        Le numéro (1-based) de la 1re ligne ajoutée dans "Team Data- URLS",
        ou None si aucune ligne n'a été ajoutée.
    """
    print("🔗 Connexion à Google Sheets…")
    source_sheet = connect_gsheet(GOOGLE_SHEET_ID, SOURCE_SHEET_NAME, service_account_info)
    target_sheet = connect_gsheet(GOOGLE_SHEET_ID, TARGET_SHEET_NAME, service_account_info)

    # --- Seuil dynamique : premier mois EXCLU ---
    today = datetime.today()
    threshold = today - relativedelta(months=MONTHS_MARGIN)
    threshold_index = _month_index(threshold.year, threshold.month)
    print(
        f"📅 Seuil : on intègre les URLs dont le mois d'intégration est < "
        f"{threshold.year}-{threshold.month:02d} (mois actuel - {MONTHS_MARGIN})."
    )

    # --- Couples (URL, mot-clé) déjà présents dans Team Data ---
    print(f"📥 Lecture de '{TARGET_SHEET_NAME}' pour la déduplication…")
    target_values = target_sheet.get_all_values()
    target_header = target_values[0] if target_values else []
    td_url_idx = _find_col(target_header, "url") or 0
    td_kw_idx = _find_col(target_header, "mc principal")
    if td_kw_idx is None:
        td_kw_idx = 2  # "MC principal optimise" attendu en 3e colonne

    existing_pairs = set()
    for row in target_values[1:]:
        if len(row) <= max(td_url_idx, td_kw_idx):
            continue
        url = row[td_url_idx].strip()
        keyword = row[td_kw_idx].strip()
        if url:
            existing_pairs.add((url, keyword))
    print(f"   {len(existing_pairs)} couples (URL, mot-clé) déjà présents.")

    # --- Lecture de la feuille source (colonnes localisées par en-tête) ---
    print(f"📥 Lecture de '{SOURCE_SHEET_NAME}'…")
    source_values = source_sheet.get_all_values()
    if not source_values:
        print("⚠️ Feuille source vide, rien à faire.")
        return None
    src_header = source_values[0]

    # URL = TOUJOURS la colonne A dans cette feuille (en-tête vide).
    # NB : on ne la détecte pas par "url" car d'autres colonnes contiennent
    # ce fragment (ex. "MC Pos sur URL avant opti").
    src_url_idx = 0
    src_cat_idx = _find_col(src_header, "categ")
    src_kw_idx = _find_col(src_header, "mc principal")
    src_integ_idx = _find_col(src_header, "integr")

    missing = [
        label
        for label, idx in [
            ("Catégorie", src_cat_idx),
            ("MC principal optimisé", src_kw_idx),
            ("Intégration", src_integ_idx),
        ]
        if idx is None
    ]
    if missing:
        raise ValueError(
            f"Colonnes introuvables dans '{SOURCE_SHEET_NAME}' : {', '.join(missing)}. "
            f"En-têtes détectés : {src_header}"
        )

    # --- Sélection des candidats ---
    new_rows = []
    seen_in_batch = set()
    for row in source_values[1:]:
        max_idx = max(src_url_idx, src_cat_idx, src_kw_idx, src_integ_idx)
        if len(row) <= max_idx:
            continue
        url = row[src_url_idx].strip()
        if not url:
            continue
        keyword = row[src_kw_idx].strip()
        category = row[src_cat_idx].strip()
        integ_raw = row[src_integ_idx].strip()

        parsed = parse_french_month(integ_raw)
        if parsed is None:
            print(f"   ⏭️  Date d'intégration illisible pour {url} : {integ_raw!r}, ignorée.")
            continue
        year, month = parsed

        # Condition : intégration < seuil (mois actuel - MONTHS_MARGIN)
        if _month_index(year, month) >= threshold_index:
            continue

        pair = (url, keyword)
        if pair in existing_pairs or pair in seen_in_batch:
            continue
        seen_in_batch.add(pair)

        date_integration = f"01/{month:02d}/{year}"
        # Colonnes A->D de Team Data : URL, Catégorie, MC principal optimise, Date Intégration
        new_rows.append([url, category, keyword, date_integration])

    if not new_rows:
        print("✅ Aucune nouvelle URL à intégrer ce mois-ci.")
        return None

    print(f"\n🆕 {len(new_rows)} URL(s) à intégrer dans '{TARGET_SHEET_NAME}' :")
    for r in new_rows:
        print(f"   • {r[0]}  |  {r[2]}  |  intégration {r[3]}")

    if dry_run:
        print("\n🧪 DRY-RUN : aucune écriture effectuée.")
        return None

    # --- Écriture : append en bas de Team Data (colonnes A->D) ---
    start_row = len(target_values) + 1
    end_row = start_row + len(new_rows) - 1
    print(f"\n📝 Écriture des lignes {start_row} à {end_row} dans '{TARGET_SHEET_NAME}'…")
    target_sheet.update(values=new_rows, range_name=f"A{start_row}:D{end_row}")

    print(f"✅ {len(new_rows)} ligne(s) ajoutée(s). 1re ligne = {start_row}.")
    return start_row


# ------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Synchronise les nouvelles URLs de 'Evolution urls optimisées' vers 'Team Data- URLS'."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Affiche les URLs qui seraient ajoutées sans rien écrire.",
    )
    cli_args = parser.parse_args()
    sync_new_urls(dry_run=cli_args.dry_run)
