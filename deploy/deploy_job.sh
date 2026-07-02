#!/usr/bin/env bash
#
# Déploiement du pipeline mensuel BPI_Creation sur GCP.
#
#   - Cloud Run Job  : exécute run_pipeline.py (image poussée par la CI GitHub).
#   - Cloud Scheduler: déclenche le job une fois par mois.
#   - Secret Manager : stocke les identifiants sensibles.
#
# Prérequis :
#   - gcloud installé et authentifié (`gcloud auth login`) avec les droits
#     Cloud Run Admin / Cloud Scheduler Admin / Secret Manager Admin.
#   - L'image a déjà été construite et poussée par le workflow GitHub
#     (.github/workflows/deploy.yml) sur Artifact Registry.
#
# Le script est IDEMPOTENT : il crée les ressources la 1re fois, les met à
# jour ensuite. Lancer depuis la racine du repo :  bash deploy/deploy_job.sh
#
set -euo pipefail

# ------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------
PROJECT_ID="eskimoz-analytics"
REGION="europe-west9"
JOB_NAME="bpicreation"
SCHEDULER_NAME="bpicreation-monthly-trigger"
IMAGE="europe-west9-docker.pkg.dev/${PROJECT_ID}/cloud-run-source-deploy/bpicreation:latest"

# Compte de service utilisé à l'exécution du job (doit pouvoir lire les secrets).
# Par défaut on réutilise le SA "google-sheet-service" déjà propriétaire des
# accès Sheets/Drive/GCS.
RUNTIME_SA="google-sheet-service@${PROJECT_ID}.iam.gserviceaccount.com"

# Compte de service qui déclenche le job depuis Cloud Scheduler.
SCHEDULER_SA="google-sheet-service@${PROJECT_ID}.iam.gserviceaccount.com"

# Planification : le 5 de chaque mois à 06h00 (heure de Paris).
CRON_SCHEDULE="0 6 5 * *"
TIMEZONE="Europe/Paris"

# Valeurs non sensibles (passées en clair).
GOOGLE_SHEET_ID_VALUE="1mN7Zrbk23Hee9p9Q5UbZdDNo263gxkacYwYVUJkZg68"
GCP_BUCKET_NAME_VALUE="semrush-debug-cache-bpi"

# ------------------------------------------------------------------
# 0. APIs nécessaires
# ------------------------------------------------------------------
echo "🔧 Activation des APIs…"
gcloud services enable \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  secretmanager.googleapis.com \
  artifactregistry.googleapis.com \
  --project "${PROJECT_ID}"

# ------------------------------------------------------------------
# 1. Secrets (à créer une seule fois — dé-commenter au 1er déploiement)
# ------------------------------------------------------------------
# Les secrets contiennent les valeurs présentes dans le fichier .env local.
# Exécuter ces commandes MANUELLEMENT la première fois (elles lisent le .env) :
#
#   # Clé de compte de service Google (JSON complet)
#   grep '^GOOGLE_CREDENTIALS_JSON' .env | sed "s/^GOOGLE_CREDENTIALS_JSON= *//" \
#     | sed "s/^'//;s/'$//" \
#     | gcloud secrets create GOOGLE_CREDENTIALS_JSON --data-file=- --project "${PROJECT_ID}"
#
#   # Clé API SEMrush
#   printf '%s' 'f48f8801557c385665692dced469e22e' \
#     | gcloud secrets create api_key_semrush --data-file=- --project "${PROJECT_ID}"
#
# Pour METTRE À JOUR un secret existant, remplacer `create` par
# `versions add`, ex :
#   printf '%s' 'NOUVELLE_CLE' | gcloud secrets versions add api_key_semrush --data-file=-
#
# Autoriser le SA d'exécution à lire les secrets :
#   for S in GOOGLE_CREDENTIALS_JSON api_key_semrush; do
#     gcloud secrets add-iam-policy-binding "$S" \
#       --member="serviceAccount:${RUNTIME_SA}" \
#       --role="roles/secretmanager.secretAccessor" --project "${PROJECT_ID}"
#   done

# ------------------------------------------------------------------
# 2. Cloud Run Job (create-or-update)
# ------------------------------------------------------------------
echo "📦 Déploiement du Cloud Run Job '${JOB_NAME}'…"
JOB_ARGS=(
  --image="${IMAGE}"
  --region="${REGION}"
  --project="${PROJECT_ID}"
  --service-account="${RUNTIME_SA}"
  --tasks=1
  --max-retries=1
  --task-timeout=3600s          # le script boucle avec sleep(1) par URL
  --memory=512Mi
  --set-env-vars="GOOGLE_SHEET_ID=${GOOGLE_SHEET_ID_VALUE},GCP_BUCKET_NAME=${GCP_BUCKET_NAME_VALUE}"
  --set-secrets="GOOGLE_CREDENTIALS_JSON=GOOGLE_CREDENTIALS_JSON_bpi:latest,api_key_semrush=api_key_semrush:latest"
)

if gcloud run jobs describe "${JOB_NAME}" --region "${REGION}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  echo "   → job existant, mise à jour."
  gcloud run jobs update "${JOB_NAME}" "${JOB_ARGS[@]}"
else
  echo "   → création du job."
  gcloud run jobs create "${JOB_NAME}" "${JOB_ARGS[@]}"
fi

# ------------------------------------------------------------------
# 3. Cloud Scheduler (create-or-update)
# ------------------------------------------------------------------
echo "⏰ Configuration du Cloud Scheduler '${SCHEDULER_NAME}'…"
RUN_URI="https://${REGION}-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/${PROJECT_ID}/jobs/${JOB_NAME}:run"

SCHED_ARGS=(
  --location="${REGION}"
  --project="${PROJECT_ID}"
  --schedule="${CRON_SCHEDULE}"
  --time-zone="${TIMEZONE}"
  --uri="${RUN_URI}"
  --http-method=POST
  --oauth-service-account-email="${SCHEDULER_SA}"
)

if gcloud scheduler jobs describe "${SCHEDULER_NAME}" --location "${REGION}" --project "${PROJECT_ID}" >/dev/null 2>&1; then
  echo "   → scheduler existant, mise à jour."
  gcloud scheduler jobs update http "${SCHEDULER_NAME}" "${SCHED_ARGS[@]}"
else
  echo "   → création du scheduler."
  gcloud scheduler jobs create http "${SCHEDULER_NAME}" "${SCHED_ARGS[@]}"
fi

echo ""
echo "✅ Déploiement terminé."
echo "   Test manuel du job :"
echo "     gcloud run jobs execute ${JOB_NAME} --region ${REGION} --project ${PROJECT_ID}"
echo "   Déclenchement manuel via le scheduler :"
echo "     gcloud scheduler jobs run ${SCHEDULER_NAME} --location ${REGION} --project ${PROJECT_ID}"
