import boto3
import os

def upload_file_to_s3(local_file_path, bucket_name, s3_key):
    """Upload a file to an S3 bucket."""
    # TODO: get the credentials from the environment variables
    AWS_ACCESS_KEY_ID = os.getenv('AWS_ACCESS_KEY_ID')
    AWS_SECRET_ACCESS_KEY = os.getenv('AWS_SECRET_ACCESS_KEY')
    AWS_SESSION_TOKEN = os.getenv('AWS_SESSION_TOKEN')
    AWS_REGION = os.getenv('AWS_REGION')
    
    session = boto3.Session(
            aws_access_key_id=AWS_ACCESS_KEY_ID,
            aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
            aws_session_token=AWS_SESSION_TOKEN,
            region_name=AWS_REGION
        )
    
    s3_client = session.client('s3')

    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        print(f"Uploaded {local_file_path} to s3://{bucket_name}/{s3_key}")
    except Exception as e:
        print(f"Failed to upload {local_file_path} to S3: {e}")
