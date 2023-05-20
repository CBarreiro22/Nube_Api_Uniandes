gcloud builds submit \
  --tag gcr.io/$GOOGLE_CLOUD_PROJECT/prueba
gcloud run deploy prueba \
  --image gcr.io/$GOOGLE_CLOUD_PROJECT/prueba \
  --platform managed \
  --region us-east1 \
  --allow-unauthenticated \
  --max-instances=1