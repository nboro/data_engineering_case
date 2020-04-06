import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 --master local[2] pyspark-shell'

import findspark; findspark.init()

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, DateType, StringType, LongType, FloatType
from pyspark.sql.window import Window

from time import sleep
import yaml
from datetime import datetime

from util import read_config

import scipy.stats

def calculate_z_score(count):
	"""
		anomaly algorithm:
		return the probability of anomaly for search volume each minute,
		calculated with the cdf from normal distribution.
	"""
	mean = 12.5
	std = 5
	probability = abs(float(scipy.stats.norm(mean,std).cdf(count)) - 0.5) / 0.5
	return probability

calculate_z_score_udf = f.udf(calculate_z_score, FloatType())

if __name__ == '__main__':

	config = read_config('config.yaml')
	search_event_topic = config['kafka']['search_event_topic']

	# start consume from kafka
	print("[INFO] Consuming from topic: " + search_event_topic)

	mySpark = SparkSession.builder.appName(search_event_topic).master("local").getOrCreate()

	# use streaming
	# use earliest offset because anomaly detection need aggregation
	df = mySpark.readStream.format("kafka"
		).option(
			"kafka.bootstrap.servers", "localhost:9092"
		).option(
			"subscribe", search_event_topic
		).option(
			"startingOffsets", "earliest"
		#).option("checkpointLocation", "kafka_checkpoint"
		#).option("failOnDataLoss", "false"
		).load()

	# data processing from csv format
	split_df = df.select(
			f.split(
				f.col("value").cast(StringType()), ','
			).alias("value")
		)

	# aggregate minutely and calculate anomaly probability
	search_df = split_df.select(
				(f.round(f.unix_timestamp(f.to_timestamp(split_df["value"][0])) / 60) * 60).cast("timestamp").alias("dt_minute")
			).groupBy(f.col("dt_minute")
			).count(
			).withColumn("probability", calculate_z_score_udf(f.col("count"))
		).select(
			f.concat(
				f.to_timestamp("dt_minute", "yyyy-MM-dd'T'HH:mm:ss'Z'"), 
				f.lit(","), 
				f.col("probability")
			).alias("value")
		)

	# stream write again to kafka
	query = search_df \
		.writeStream \
		.format("kafka") \
		.option("kafka.bootstrap.servers", "localhost:9092") \
		.option("checkpointLocation", "cp4")\
		.option("topic", config["kafka"]["search_anomaly_topic"]) \
		.outputMode("update") \
		.start()

	query.awaitTermination()