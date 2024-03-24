from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType

hdfs_output_directory = "hdfs://Master:9000/user/hadoop/outputs/spark_outputs/weatherstreaming/data"
checkpoint_directory = "hdfs://Master:9000/user/hadoop/outputs/spark_outputs/weatherstreaming/checkpoints"

schema = StructType([
    StructField("CityName", StringType(), True),
    StructField("Temperature", DoubleType(), True),
    StructField("Humidity", DoubleType(), True),
    StructField("RainIntensity", IntegerType(), True),
    StructField("WindSpeed", DoubleType(), True),
    StructField("WindDirection", DoubleType(), True),
    StructField("CreationTime", TimestampType(), True)
])


spark = SparkSession.builder.appName("KafkaWeatherDataStream").getOrCreate()
spark.sparkContext.setLogLevel("WARN")


df = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "Master:9092,Worker1:9092,Worker2:9092") \
    .option("subscribe", "weathertopic") \
    .option("startingOffsets", "earliest") \
    .load()


parsed_df = df.selectExpr("CAST(value AS STRING)").select(from_json("value", schema).alias("json_value")).select("json_value.*")
console_query = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .trigger(processingTime='60 seconds') \
    .start()

# we can add .outputMode("append") \
#.option("header", "true") \
hdfs_query = parsed_df.writeStream \
    .format("csv") \
    .outputMode("append") \
    .option("checkpointLocation", checkpoint_directory) \
    .option("timestampFormat", "yyyy-MM-dd HH:mm") \
    .option("path", hdfs_output_directory) \
    .trigger(processingTime='60 seconds') \
    .start()

console_query.awaitTermination()
hdfs_query.awaitTermination()
