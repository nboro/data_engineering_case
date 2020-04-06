from kafka import KafkaProducer
from pymongo import MongoClient
import numpy as np

import os
import time
import csv
from datetime import datetime
from sys import exit

from util import read_config

def on_send_success(record_metadata):
	"""
		function for async success write to kafka
	"""
	print("[INFO] Successfully sent: (topic={0}) (partition={1}) (ofsset={2})".format(
		record_metadata.topic, record_metadata.partition, record_metadata.offset
	))

def on_send_error(exception):
	"""
		function for async failed write to kafka
	"""
	print("[ERROR] Failed to send: " + exception)

if __name__=="__main__":
	config = read_config('config.yaml')

	input_file_name = config['backend']['input']

	# check if input file exists
	if not os.path.isfile(input_file_name):
		exit("[ERROR] input file not found")

	# connect to mongo
	client = MongoClient('localhost', 27017)
	property_col = client[config['mongo']['db_name']].properties

	# create producer for kafka
	producer = KafkaProducer(bootstrap_servers='localhost:9092')
	target_topic = config['kafka']['search_event_topic']

	# start reading input file
	with open(input_file_name, 'r') as input_file:
		search_reader = csv.DictReader(input_file)
		for line in search_reader:
			# reload message for delay time
			config = read_config('config.yaml')

			# construct message
			current_dt = datetime.now().strftime("%Y-%m-%dT%H:%M:%SZ")
			user_id = line['user_id']
			property_info = property_col.find_one({'_id' : int(line['property_id'])})
			neighbourhood = property_info['neighbourhood'] if property_info else 'N/A'
			room_type = property_info['room_type'] if property_info else 'N/A'
			search_event = ','.join([current_dt, user_id, neighbourhood, room_type])

			# send async to kafka
			producer.send(
				target_topic, str.encode(search_event)
			).add_callback(
				on_send_success
			).add_errback(
				on_send_error
			)

			# delay to simulate search frequency
			delay = config['backend']['delay']
			time.sleep(max(delay['min'], np.random.normal(delay['mu'], delay['std'])))