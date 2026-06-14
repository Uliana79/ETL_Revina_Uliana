from pyspark.sql.types import *
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("airline-delay-cause") \
    .enableHiveSupport() \
    .getOrCreate()

schema = StructType([
    StructField('year', IntegerType(), True),
    StructField('month', IntegerType(), True),
    StructField('carrier', StringType(), True),
    StructField('carrier_name', StringType(), True),
    StructField('airport', StringType(), True),
    StructField('airport_name', StringType(), True),
    StructField('arr_flights', IntegerType(), True),
    StructField('arr_del15', IntegerType(), True),
    StructField('carrier_ct', DoubleType(), True),
    StructField('weather_ct', DoubleType(), True),
    StructField('nas_ct', DoubleType(), True),
    StructField('security_ct', DoubleType(), True),
    StructField('late_aircraft_ct', DoubleType(), True),
    StructField('arr_cancelled', IntegerType(), True),
    StructField('arr_diverted', IntegerType(), True),
    StructField('arr_delay', IntegerType(), True),
    StructField('carrier_delay', IntegerType(), True),
    StructField('weather_delay', IntegerType(), True),
    StructField('nas_delay', IntegerType(), True),
    StructField('security_delay', IntegerType(), True),
    StructField('late_aircraft_delay', IntegerType(), True)
])

df = spark.read \
    .option("header", "true") \
    .option("delimiter", ",") \
    .schema(schema) \
    .csv("s3a://revina/input_data/Airline_Delay_Cause_fixed.csv")

df.write.mode("overwrite").option("path","s3a://revina/airline_delay_cause").saveAsTable("airline_delay_cause")