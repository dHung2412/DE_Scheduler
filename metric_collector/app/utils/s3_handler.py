import os
from dotenv import load_dotenv
import boto3
from datetime import datetime
from botocore.exceptions import NoCredentialsError, ClientError
import logging

load_dotenv(override=True)

class S3Handler:
    def __init__(self):
        self.endpoint = os.getenv("MINIO_ENDPOINT")
        self.s3_client = boto3.client(
            's3',
            endpoint_url=self.endpoint,
            aws_access_key_id=os.getenv("MINIO_ROOT_USER"),
            aws_secret_access_key=os.getenv("MINIO_ROOT_PASSWORD")
        )
        self.bucket = os.getenv("MINIO_BUCKET")

    def check_bucket(self) -> bool:
        try:
            self.s3_client.head_bucket(Bucket=self.bucket)
            return True
        except ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code in ["404", "NoSuchBucket"]:
                return False
            raise
    
    def upload_data(self, data_bytes, filename):
        today = datetime.utcnow()
        object_name = f"landing_zone/{today:%Y/%m/%d}/{filename}"

        try:
            self.s3_client.put_object(
                Bucket=self.bucket,
                Key=object_name,
                Body=data_bytes
            )                  
            return True
        except ClientError as e:
            print(f"-----> [S3] Upload file failed: {e}")
            return False
