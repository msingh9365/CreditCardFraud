from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType
import os

FRAUD_OUTPUT_PATH = os.environ.get('FRAUD_OUTPUT_PATH')
NON_FRAUD_OUTPUT_PATH = os.environ.get('NON_FRAUD_OUTPUT_PATH')
CHECKPOINT_DIR = os.environ.get('CHECKPOINT_DIR')
KAFKA_BOOTSTRAP_SERVER = os.environ.get('KAFKA_BOOTSTRAP_SERVER')
KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC')
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID')
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY')


def process_batch(df: DataFrame, _):
    fraud_df = df.filter(col("is_fraud") == 1)
    non_fraud_df = df.filter(col("is_fraud") == 0)

    if fraud_df.count() > 0:
        fraud_df.write.format("json").mode("append").option("path", FRAUD_OUTPUT_PATH).save()

    if non_fraud_df.count() > 0:
        non_fraud_df.write.format("json").mode("append").option("path", NON_FRAUD_OUTPUT_PATH).save()


def read_from_kafka(spark):
    # Define the schema
    schema = StructType([
        StructField("trans_date_trans_time", StringType(), True),
        StructField("merchant", StringType(), True),
        StructField("category", StringType(), True),
        StructField("amt", DoubleType(), True),
        StructField("city", StringType(), True),
        StructField("state", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("long", DoubleType(), True),
        StructField("city_pop", IntegerType(), True),
        StructField("job", StringType(), True),
        StructField("dob", StringType(), True),
        StructField("trans_num", StringType(), True),
        StructField("merch_lat", DoubleType(), True),
        StructField("merch_long", DoubleType(), True),
        StructField("is_fraud", IntegerType(), True)
    ])

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()
    # .option("startingOffsets", "earliest")  add this to consume topic from the beginning

    converted_df = df.select(col("value").cast("string").alias("json")) \
        .select(from_json(col("json"), schema).alias("data")) \
        .select('data.*')

    query = converted_df.writeStream \
        .foreachBatch(process_batch) \
        .option('checkpointLocation', CHECKPOINT_DIR) \
        .outputMode('append') \
        .start()

    query.awaitTermination()


def main():
    spark = SparkSession \
        .builder \
        .appName("CreditCardFraudApp") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("spark.hadoop.fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .config("spark.hadoop.fs.s3a.bucket.auto.create", "true") \
        .config('spark.hadoop.fs.s3a.aws.credentials.provider',
                'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()

    read_from_kafka(spark)


if __name__ == "__main__":
    main()
