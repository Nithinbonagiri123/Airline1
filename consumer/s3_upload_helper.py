import boto3

def push_to_cloud_storage(file_path, bucket, object_key):
    """
    Store a local file in an S3 bucket under a specified key.
    Args:
        file_path (str): Path to the file on the local filesystem.
        bucket (str): Name of the destination S3 bucket.
        object_key (str): S3 object key (path in bucket).
    """
    s3_resource = boto3.client('s3')
    try:
        s3_resource.upload_file(file_path, bucket, object_key)
        print(f"[S3 UPLOAD] Success: {file_path} → s3://{bucket}/{object_key}")
    except Exception as exc:
        print(f"[S3 UPLOAD] Error uploading {file_path} to {bucket}/{object_key}: {exc}")
