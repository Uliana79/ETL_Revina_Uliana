#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import to_json, struct, col

def main():
    spark = SparkSession.builder.appName("kafka-write-json").getOrCreate()

    df = spark.read.option("multiline", "true").json("s3a://revina/input/github_issues_tickets.json")

    df.printSchema()

    kafka_df = df.select(to_json(struct("*")).alias("value"))

    kafka_df.write \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-720j9kjlod0mh9gn.mdb.yandexcloud.net:9091") \
        .option("topic", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1;") \
        .save()

if __name__ == "__main__":
    main()