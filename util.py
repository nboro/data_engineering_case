import os
import yaml

def read_config(fname):
	# check if file exists
	if not os.path.isfile(fname):
		return None

	# read config file
	with open(fname, 'r') as config_file:
		config = yaml.load(config_file)
	return config