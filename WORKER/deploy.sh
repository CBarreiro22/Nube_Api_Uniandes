gcloud builds submit \
  --tag gcr.io/$GOOGLE_CLOUD_PROJECT/prueba
gcloud run deploy prueba \
  --image gcr.io/$GOOGLE_CLOUD_PROJECT/prueba \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --max-instances=1 \
  --port=5000 \
  --set-env-vars "DB_HOST=10.188.0.4" \
  --set-env-vars "DB_USER=admin" \
  --set-env-vars "DB_PASSWORD=admin" \
  --set-env-vars "DB_NAME=apisnube" \
  --set-env-vars "DB_PORT=5432"

