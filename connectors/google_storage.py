from google.cloud import storage

def upload_to_gcs(bucket_name: str, source_file: str, destination_blob: str):
    """Upload un fichier local (CSV ou log) vers un bucket GCS."""
    try:
        client = storage.Client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(source_file)
        print(f"📤 Fichier {source_file} uploadé vers gs://{bucket_name}/{destination_blob}")
    except Exception as e:
        print(f"⚠️ Erreur upload GCS : {e}")
