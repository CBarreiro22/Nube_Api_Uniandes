gcloud builds submit \
  --tag gcr.io/$GOOGLE_CLOUD_PROJECT/prueba
gcloud run deploy prueba \
  --image gcr.io/$GOOGLE_CLOUD_PROJECT/prueba \
  --platform managed \
  --region us-central1 \
  --allow-unauthenticated \
  --max-instances=1 \
#  --port=5000
#gcloud compute firewall-rules create prueba-cloud \
#--allow=tcp:5432 \
#--destination-ranges=10.188.0.0/24 \
#--direction=EGRESS \
#--network=red-new \
#--target-tags=vpc-tag

