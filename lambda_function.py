import os
import datetime as dt
import pytz
import boto3
import json

from utils import runReport

from botocore.exceptions import ClientError

import calendar 

client = boto3.client('s3')

def lambda_handler(event, context):
        
    today = dt.datetime.now(pytz.timezone('US/Eastern')).date()

    start_date_str = today.strftime("%Y-%m-%d")
    year = today.year
    month = today.month

    end_date = today + dt.timedelta(days=calendar.monthrange(year,month)[1]-1)
    end_date_str = end_date.strftime("%Y-%m-%d")
    
    COMMUNITIES = os.environ.get("COMMUNITIES").split("")

    S3_BUCKET_NAME = 'revenue-tracking-dev'

    ## place pdf into s3 bucket
    
    for community in COMMUNITIES:
        filepath = runReport.create_rev_metric_report(community, start_date_str, end_date_str, client)

        # Get file name only excluding prefix
        key = filepath.split('/')[2]
        
        # Get file from temp and store in s3
        response = client.put_object(
            Body = open(filepath, 'rb'),
            ContentType = 'application/pdf',
            Bucket = S3_BUCKET_NAME,
            Key = key)

    return response
