from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, ArrayType
from pyspark.sql.functions import col, from_json, regexp_replace, expr
from pyspark.sql import DataFrame

FRAUD_OUTPUT_PATH = "/opt/spark/data/CreditCardTrans/fraud_trans"
NON_FRAUD_OUTPUT_PATH = "/opt/spark/data/CreditCardTrans/non_fraud_trans"
CHECKPOINT_DIR = "/opt/spark/data/checkpoint/stream"
KAFKA_BOOTSTRAP_SERVER = "kafka:9092"
KAFKA_TOPIC = "kf.topic.creditCard.transactions"

def process_batch(df: DataFrame, epoch_id):
    fraud_df = df.filter(col("is_fraud") == 1)
    non_fraud_df = df.filter(col("is_fraud") == 0)

    if fraud_df.count() > 0:
        fraud_df.write.format("json").mode("append").option("path", FRAUD_OUTPUT_PATH).save()

    else:
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
    spark = SparkSession.builder.appName("CreditCardFraudApp").getOrCreate()
    read_from_kafka(spark)

if __name__ == "__main__" :
    main()