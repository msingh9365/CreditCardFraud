##Execution Steps

docker exec -ti spark \
spark-submit --deploy-mode client \
--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
/home/spark/jobs/kafka_ingestion_app.py

