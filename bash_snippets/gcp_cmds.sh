gcloud auth activate-service-account --key-file=key-file.json

gsutil mb -c "public-read" gs://my-bucket

gcloud compute networks create my-vpc --subnet custom --subnetworks='vpc-subnet1=region-us-central1/vpc-subnet1"

gcloud compute networks subnets create my-vpc-subnet1 --network my-vpc --region us-cental1 --range 10.0.0.0/16

gcloud builds submit --tag gcr.io/my-project/my-image --config cloudbuild.yaml

gcloud run services deploy my-service --image gcr.io/my-project/my-image --platform --region us-central

bq load --source_format=CSV --autodectect dataset.table dataset.table gs://bucket/source.txt

gcloud container clusters create my-cluster --zone us-central1-a

kubectl apply -f deployment.yaml

gcloud dataproc clusters create my-dataproc-cluster --region us-central1 --master-machine-type n1-standard-2 --num-workers 2

gcloud dataproc jobs submit pyspark --cluster my-dataproc-cluster --jar gs://dataproc-jars/google-cloud-dataproc/hadoop-common/current/hadoop-common-examples.jar --class org.apache.hadoop.examples.pi --args 10000
