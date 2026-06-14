#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, concat, lit, array, to_json
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, 
    ArrayType, BooleanType, LongType, NullType
)

def main():
    spark = SparkSession.builder.appName("kafka-github-flatten-full").config("spark.sql.streaming.schemaInference", "true").getOrCreate()

    github_schema = StructType([
        StructField("url", StringType(), True),
        StructField("comments_url", StringType(), True),
        StructField("html_url", StringType(), True),
        StructField("id", LongType(), True),
        StructField("title", StringType(), True),
        
        StructField("user", StructType([
            StructField("login", StringType(), True),
            StructField("id", LongType(), True),
            StructField("url", StringType(), True),
            StructField("type", StringType(), True)
        ]), True),
        
        StructField("state", StringType(), True),
        
        StructField("labels", ArrayType(StructType([
            StructField("id", LongType(), True),
            StructField("url", StringType(), True),
            StructField("name", StringType(), True),
            StructField("color", StringType(), True),
            StructField("default", BooleanType(), True),
            StructField("description", StringType(), True)
        ])), True),
        
        StructField("assignee", NullType(), True),
        StructField("milestone", StructType([
            StructField("title", StringType(), True),
            StructField("description", StringType(), True),
            StructField("due_on", NullType(), True)
        ]), True),
        
        StructField("comments", IntegerType(), True),
        StructField("created_at", StringType(), True),
        StructField("updated_at", StringType(), True),
        StructField("closed_at", StringType(), True),
        StructField("body", StringType(), True),
        
        StructField("reactions", StructType([
            StructField("url", StringType(), True),
            StructField("total_count", IntegerType(), True),
            StructField("plus_1", IntegerType(), True),
            StructField("minus_1", IntegerType(), True),
            StructField("laugh", IntegerType(), True),
            StructField("hooray", IntegerType(), True),
            StructField("confused", IntegerType(), True),
            StructField("heart", IntegerType(), True),
            StructField("rocket", IntegerType(), True),
            StructField("eyes", IntegerType(), True)
        ]), True),
        
        StructField("state_reason", StringType(), True),
        
        StructField("answers", ArrayType(StructType([
            StructField("author", StringType(), True),
            StructField("body", StringType(), True),
            StructField("creation_time", StringType(), True)
        ])), True),
        
        StructField("repo_name", StringType(), True)
    ])

    streaming_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "rc1b-720j9kjlod0mh9gn.mdb.yandexcloud.net:9091") \
        .option("subscribe", "dataproc-kafka-topic") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "SCRAM-SHA-512") \
        .option("kafka.sasl.jaas.config",
                "org.apache.kafka.common.security.scram.ScramLoginModule required "
                "username=user1 "
                "password=password1;") \
        .option("startingOffsets", "earliest") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json_string") \
        .where(col("json_string").isNotNull())

    flattened_df = streaming_df \
        .select(from_json(col("json_string"), github_schema).alias("data")) \
        .select(
            col("data.id").alias("issue_id"),
            col("data.url").alias("issue_url"),
            col("data.html_url").alias("issue_html_url"),
            col("data.title").alias("title"),
            col("data.state").alias("state"),
            col("data.state_reason").alias("state_reason"),
            col("data.comments").alias("comments_count"),
            col("data.body").alias("body"),
            col("data.repo_name").alias("repo_name"),
            
            col("data.user.login").alias("user_login"),
            col("data.user.id").alias("user_id"),
            col("data.user.url").alias("user_url"),
            col("data.user.type").alias("user_type"),
            
            col("data.milestone.title").alias("milestone_title"),
            col("data.milestone.description").alias("milestone_description"),
            
            col("data.reactions.total_count").alias("reactions_total_count"),
            col("data.reactions.plus_1").alias("reactions_plus_one"),
            col("data.reactions.minus_1").alias("reactions_minus_one"),
            col("data.reactions.laugh").alias("reactions_laugh"),
            col("data.reactions.hooray").alias("reactions_hooray"),
            col("data.reactions.confused").alias("reactions_confused"),
            col("data.reactions.heart").alias("reactions_heart"),
            col("data.reactions.rocket").alias("reactions_rocket"),
            col("data.reactions.eyes").alias("reactions_eyes"),
            
            to_timestamp("data.created_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("created_at"),
            to_timestamp("data.updated_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("updated_at"),
            to_timestamp("data.closed_at", "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("closed_at"),
            
            to_json("data.labels").alias("labels_json"),
            to_json("data.answers").alias("answers_json")
        )

    query = flattened_df.writeStream \
            .trigger(once=True) \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "s3a://revina/github_issues_flattened") \
            .option("checkpointLocation", "s3a://revina/checkpoints/github_flattened") \
            .start()

    query.awaitTermination()

if __name__ == "__main__":
    main()