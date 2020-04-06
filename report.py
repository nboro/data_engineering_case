from kafka import KafkaConsumer

import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 --master local[2] pyspark-shell'

import findspark; findspark.init()
from pyspark.sql import SparkSession
import pyspark.sql.functions as f

import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql.functions  import date_format
from pyspark.sql import Row

import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from util import read_config
from email_sender import send

if __name__=="__main__":
    #read configurations file
    config = read_config('config.yaml')

    #set the topic for kafka subscription
    search_event_topic = config['kafka']['search_event_topic']

    #make spark session
    mySpark = SparkSession.builder.appName("search_properties").master("local").getOrCreate()

    #subscribe to topic search_event_topic and store it to spark dataframe
    df = mySpark \
            .read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", search_event_topic) \
            .option("startingOffsets", "earliest") \
            .load()

    #This is ETL. The output is a new spark dataframe containing 4 columns. We do aggregation and to find out search volume per user	
    searches = df \
         .selectExpr("split(value,',')[0] as dt" \
                    ,"split(value,',')[1] as user_id" \
                    ,"split(value,',')[2] as neighbourhood" \
                    ,"split(value,',')[3] as room_type"\
                    )\
         .withColumn('date', date_format('dt', "yyyy-MM-dd"))\
         .groupby(['date', 'neighbourhood', 'room_type'])\
         .agg(f.count(f.col('user_id')).alias('search_count'))

    #write previous dataframe to mongodb collection. Database name, collection name retrieved from configuration file	 
    searches.write\
            .format("com.mongodb.spark.sql.DefaultSource")\
            .option("uri","mongodb://127.0.0.1/{0}.{1}".format(
                config['mongo']['db_name'], 
                config['mongo']['search_summary_collection']))\
            .mode('append')\
            .save()

    #read search summary from mongodb and store it to spark dataframe
    readUsers = mySpark.read\
            .format("com.mongodb.spark.sql.DefaultSource")\
            .option("uri","mongodb://127.0.0.1/{0}.{1}".format(
                config['mongo']['db_name'],
                config['mongo']['search_summary_collection']
            )).load()

    #create a view for the previously created dataframe. Store to new spark dataframe results 
    readUsers.createOrReplaceTempView("users")
    sqlDF = mySpark.sql("SELECT * FROM users")

    #Report creation procedure initialization. Convert spark dataframe to pandas, convert column date to datetime (format: YYYY-MM-DD)	
    pdf1=sqlDF.toPandas()
    pdf1['date'] = pd.to_datetime(pdf1['date'])
    
    #select search volume for the last 3 months, plot resuts
    pdf2 = pdf1.groupby(['date']).sum().last('3M')
    line = pdf2.plot(colormap='winter_r', figsize=(7.5,5), legend=False)
    line.set_ylabel("Search Volume")

    #Save plot as image
    fig = line.get_figure()
    fig.savefig(config['report']['output_image_name'])

    # Send report via email. Plot image will in the attachment and in image body
    send(
        email=config['report']['email_addr'],
        pw=config['report']['pwd'],
        subject=config['report']['subject'],
        to_addr=config['report']['target_email_addr'],
        output_image_name=config['report']['output_image_name']
    )