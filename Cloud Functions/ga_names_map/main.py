import os
import sys
import logging
from google.cloud import storage
import pandas as pd
import json
from oauth2client.service_account import ServiceAccountCredentials
from google.oauth2 import service_account
import google.auth

#bucket_name_from = "offline_test"
bucket_name_to = "bucket_to"
crm_data_name = "data.csv"
data_map_location = "names_map.csv"

def download_gcs_file(obj, to, bucket):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(obj)

    blob.download_to_filename(to)
    logging.debug('downloaded file {} to {}'.format(obj, to))

def data_names_map(event, context): 
	"""Triggered by a change to a Cloud Storage bucket.
	Args:
		event (dict): Event payload.
		context (google.cloud.functions.Context): Metadata for the event.
	"""

	file_data = event
	file_name = file_data['name']


	if (file_name != crm_data_name):
		sys.exit()
	

	bucket_name_from = file_data['bucket']
	print(f"Processing file: {file_name}.")
	print(f"From Bucket : {bucket_name_from}. ")
	
	#Create storage client
	try: 
		storage_client = storage.Client()
	except:
		print("Error is with storage_client")
	#Read crm and map data and rename column names 
	try:

		#read data map as cvs. Data map should be a data.frame with coloumn names name and id, where id is in the form ga:dimensionXX 
		map = pd.read_csv("gs://"+bucket_name_from+"/"+data_map_location)
		#map = pd.Series(map.id.values, index = map.name).to_dict()


		print("next log is names map:->")
		print(map)

		#read crm data as csv
		data = pd.read_csv("gs://"+bucket_name_from+"/"+crm_data_name, header = 0, names = map.id.to_list())

		print("next log is initial column names:->")
		print(list(data.columns))

		#Change crm coloumn names to ga compatible names, ready for data import.
		#data = data.rename(columns = map)
		#print("Next log is column names:->")
		#print(list(data.columns))
	except: 
		print("error is reading csv form storage")
	#Then put this data into a separate storage bucket.
	#create storage client
	
	#Upoad data to ga bucket
	try:
		bucket = storage_client.get_bucket(bucket_name_to)
		blob = bucket.blob(crm_data_name)

		#change pandas table to readable csv
		df = pd.DataFrame(data)
		df = pd.DataFrame(data=data).to_csv(index = False)
		
		#upload data
		return blob.upload_from_string(data=df, content_type='text/csv')
	except: 
		print("Error: Couldnt upload data to file")

#data_names_map()