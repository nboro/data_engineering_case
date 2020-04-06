Stream and batch processing implementation
This was created as part of an assignment for the Data Engineering course of the MSc Data Science and Entrepreneurship program
Tools used: Apache Spark, Apache Kafka, MongoDB and ELK Stack
Content:
	FILES
	- backend.py		: simulator for backend as a data source
	- config.yaml		: contains all the configuration accessed by batch and real-time service
	- consumer.py		: anomaly detection implementation
	- email_sender.py	: SMTP function for daily report
	- report.py			: batch implementation to generate daily report
	- util.py			: contains read_config utility function

	FOLDERS
	- data 				: contains 2 files as input for the backend.py
	- logstash			: all necessary bash and config file to run logstash
	- mongo				: contains data and bash file to populate designated mongo collection for daily report


Required library:
	1. spark-mongo connector (.jar)

Required services:
	1. Run mongod
	2. Run zookeeper
	3. Run kafka
	4. Run elasticsearch
	5. Run kibana
	6. Run logstash for search_events
	7. Run logstash for search_anomalies