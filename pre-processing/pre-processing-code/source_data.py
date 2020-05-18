import os
import boto3
from urllib.request import urlopen
from urllib.error import URLError, HTTPError
import csv
import json

def source_dataset(new_filename, s3_bucket, new_s3_key):

	source_dataset_url = 'https://data.cdc.gov/api/views/b58h-s9zx/rows.csv'
	file_location = '/tmp/' + new_filename

	try:
		response = urlopen(source_dataset_url)

	except HTTPError as e:
		raise Exception('HTTPError: ', e.code, source_dataset_url)

	except URLError as e:
		raise Exception('URLError: ', e.reason, source_dataset_url)

	else:

		data = response.read().decode().splitlines()
		fieldnames = data[0].lower().replace(' ', '_')

		with open(file_location + '.csv', 'w', encoding='utf-8') as c:
		
			c.write(fieldnames + '\n')
			c.write('\n'.join((datum) for datum in data[1:]))

		with open(file_location + '.csv', 'r') as r, open(file_location + '.json', 'w', encoding='utf-8') as j:
			reader = csv.DictReader(r)
			j.write('[')
			j.write(',\n'.join(json.dumps(row) for row in reader))
			j.write(']')

	asset_list = []

	# Creates S3 connection
	s3 = boto3.client('s3')

	# Looping through filenames, uploading to S3
	for filename in os.listdir('/tmp'):

		s3.upload_file('/tmp/' + filename, s3_bucket, new_s3_key + filename)
		asset_list.append({'Bucket': s3_bucket, 'Key': new_s3_key + filename})
		os.remove('/tmp/' + filename)

	return asset_list
