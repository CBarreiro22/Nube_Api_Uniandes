gcloud builds submit \
  --tag gcr.io/$GOOGLE_CLOUD_PROJECT/prueba
gcloud run deploy prueba \
  --image gcr.io/$GOOGLE_CLOUD_PROJECT/prueba \
  --platform managed \
  --region northamerica-northeast2 \
  --allow-unauthenticated \
  --max-instances=1 \
  --port=5000
