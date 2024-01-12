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
    """
    os.environ['AWS_ACCESS_KEY_ID'] = access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_access_key_id

    s3_client = boto3.client('s3')
    response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    return response['Body'].read()

def upload_s3_object(access_key_id, secret_access_key_id, bucket_name, object_key, file_path):
    """
    Upload an object to an S3 bucket.
    """
    os.environ['AWS_ACCESS_KEY_ID'] = access_key_id
    os.environ['AWS_SECRET_ACCESS_KEY'] = secret_access_key_id

    s3_client = boto3.client('s3')

    try:
        s3_client.upload_file(file_path, bucket_name, object_key)
    except Exception as e:
        print(f"Error uploading file '{file_path}' to '{bucket_name}': {e}")
