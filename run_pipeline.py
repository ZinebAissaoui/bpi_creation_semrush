"""
Orchestrateur mensuel du pipeline BPI_Creation.

Enchaîne, dans l'ordre :
  1. sync_urls.sync_new_urls()  -> intègre dans "Team Data- URLS" les nouvelles
     URLs de "Evolution urls optimisées" (intégration < mois actuel - 2, dédup
     par couple URL+mot-clé) et renvoie la 1re ligne ajoutée.
  2. main.main(start_row=...)   -> calcule les positions SEMrush M-1/M+1 pour
     UNIQUEMENT ces nouvelles lignes (si des URLs ont été ajoutées).
  3. main_monthly.main()        -> snapshot mensuel des positions dans "monthly_run".

Point d'entrée du conteneur (voir dockerfile). Sort avec un code != 0 en cas
d'erreur pour que Cloud Run marque l'exécution en échec.
"""
import sys
import traceback

from sync_urls import sync_new_urls
import main
import main_monthly


def run():
    print("=" * 70)
    print("🚀 Démarrage du pipeline mensuel BPI_Creation")
    print("=" * 70)

    # 1. Synchronisation Evolution -> Team Data
    print("\n[1/3] Synchronisation des nouvelles URLs…")
    first_new_row = sync_new_urls()

    # 2. Positions M-1/M+1 pour les nouvelles lignes uniquement
    if first_new_row:
        print(f"\n[2/3] Calcul SEMrush M-1/M+1 à partir de la ligne {first_new_row}…")
        main.main(start_row=first_new_row)
    else:
        print("\n[2/3] Aucune nouvelle URL intégrée : étape M-1/M+1 sautée.")

    # 3. Snapshot mensuel
    print("\n[3/3] Snapshot mensuel (monthly_run)…")
    main_monthly.main()

    print("\n" + "=" * 70)
    print("✅ Pipeline mensuel terminé avec succès.")
    print("=" * 70)


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        print(f"\n❌ Échec du pipeline : {exc}", file=sys.stderr)
        traceback.print_exc()
        sys.exit(1)
