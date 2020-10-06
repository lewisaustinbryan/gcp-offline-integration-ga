import logging
import base64
import json
import os
from urllib.error import HTTPError
from apiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from apiclient.http import MediaFileUpload
from google.cloud import storage


ACCOUNTID='XXXXXXXX'
WEBPROPERTYID='UA-XXXXXXXX-X'  
CUSTOM_DATASOURCE_ID='XXXXXXXXXXXXXXXX'
FOLDER='bucket_from'

# save file to /tmp only as cloud functions only supports that to write to
def download_gcs_file(obj, to, bucket):
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(obj)

    blob.download_to_filename(to)
    logging.debug('downloaded file {} to {}'.format(obj, to))

# needs an auth.json file as cloud auth not working for analytics requests
def get_ga_service(bucket):
    
    download_gcs_file('client_secret.json', '/tmp/client_secret.json', bucket)
    credentials = ServiceAccountCredentials.from_json_keyfile_name('/tmp/client_secret.json',scopes=['https://www.googleapis.com/auth/analytics','https://www.googleapis.com/auth/analytics.edit'])

    # Build the service object.
    return build('analytics', 'v3', credentials=credentials, cache_discovery=False)
    
def upload_ga(obj_name, bucket):

    filename = '/tmp/{}'.format(os.path.basename(obj_name))
    logging.info("filename: {}".format(filename))
    
    download_gcs_file(obj_name, filename, bucket)

    try:
        analytics = get_ga_service(bucket)
        logging.info("get_ga_service works")
    except TypeError as error:
        logging.error("There was an error creating GA service : {}".format(error))

    try:
        media = MediaFileUpload(filename,
                                    mimetype='application/octet-stream',
                                    resumable=False)
    except TypeError as error:
        logging.error("MediaFileUpload didnt work: {}".format(error))

    try:  
        daily_upload = analytics.management().uploads().uploadData(accountId=ACCOUNTID,webPropertyId=WEBPROPERTYID,customDataSourceId=CUSTOM_DATASOURCE_ID,media_body=media).execute()
        logging.info('Uploaded file: {}'.format(json.dumps(daily_upload)))
    except TypeError as error:
        # Handle errors in constructing a query.
        logging.error('There was an error in constructing your query : {}'.format(error))
    except HTTPError as error:
        # Handle API errors.
        logging.error('There was an API error : {} : {}'.format(error.resp.status, error.resp.reason))

    

def gcs_to_ga(data, context):
    """Background Cloud Function to be triggered by Pub/Sub subscription.
       This functions copies the triggering BQ table and copies it to an aggregate dataset.
    Args:
        data (dict): The Cloud Functions event payload.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to Stackdriver Logging
    """

    logging.info('Bucket: {} File: {} Created: {} Updated: {}'.format(data['bucket'],
                                                                        data['name'],
                                                                        data['timeCreated'],
                                                                        data['updated']))
    folder = FOLDER
    object_name = data['name']
    bucket = data['bucket']

    logging.info('File matches folder {}'.format(folder))

    return upload_ga(object_name, bucket)

    

