gcloud dataproc jobs submit spark \
       --cluster=my-cluster \
       --class=com.example.MyPipeline \
       --jars=gs://my-bucket/my-pipeline.jar \
       -- \
       --input=gs://my-bucket/input \
       --output=gs://my-bucket/output
