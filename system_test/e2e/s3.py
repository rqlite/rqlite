#!/usr/bin/env python

import boto3
import os

def delete_s3_object(access_key_id, secret_access_key_id, bucket_name, object_key):
    """
    Delete an object from an S3 bucket.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_access_key_id

    s3_client = boto3.client('s3')
    s3_client.delete_object(Bucket=bucket_name, Key=object_key)

def download_s3_object(access_key_id, secret_access_key_id, bucket_name, object_key):
    """
    Download an object from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        object_key (str): The key of the object to download.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_access_key_id

    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    return response['Body'].read()