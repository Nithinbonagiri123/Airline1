import boto3
import logging

def transfer_to_cloud_storage(local_data_file, target_bucket, cloud_storage_path):
    """
    Transfer a local data file (e.g., airline customer review analytics results) to a cloud storage bucket at the specified path.
    Args:
        local_data_file (str): Filesystem path to the local data file.
        target_bucket (str): Name of the cloud storage bucket.
        cloud_storage_path (str): Destination path within the bucket.
    """
    logger = logging.getLogger("cloud_storage_helper")
    cloud_storage_client = boto3.client('s3')
    try:
        cloud_storage_client.upload_file(local_data_file, target_bucket, cloud_storage_path)
        logger.info(f"Data file transferred to cloud storage: {local_data_file} -> {target_bucket}/{cloud_storage_path}")
    except Exception as transfer_error:
        logger.error(f"Cloud storage transfer failed for {local_data_file} to {target_bucket}/{cloud_storage_path}: {transfer_error}")
