import boto3
from botocore.exceptions import ClientError
from config import settings
import logging

logger = logging.getLogger(__name__)


class S3Client:
    def __init__(self):
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=settings.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=settings.AWS_SECRET_ACCESS_KEY,
            region_name=settings.AWS_REGION
        )
        self.bucket_name = settings.S3_BUCKET_NAME
    
    def generate_presigned_upload_url(self, file_key: str, content_type: str = "application/pdf") -> str:
        try:
            presigned_url = self.s3_client.generate_presigned_url(
                'put_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': file_key,
                    'ContentType': content_type
                },
                ExpiresIn=settings.PRESIGNED_URL_EXPIRATION
            )
            return presigned_url
        except ClientError as e:
            logger.error(f"Error generating presigned upload URL: {e}")
            raise
    
    def generate_presigned_download_url(self, file_key: str) -> str:
        try:
            presigned_url = self.s3_client.generate_presigned_url(
                'get_object',
                Params={
                    'Bucket': self.bucket_name,
                    'Key': file_key
                },
                ExpiresIn=settings.DOWNLOAD_URL_EXPIRATION
            )
            return presigned_url
        except ClientError as e:
            logger.error(f"Error generating presigned download URL: {e}")
            raise
    
    def download_file(self, file_key: str, local_path: str) -> None:
        try:
            self.s3_client.download_file(self.bucket_name, file_key, local_path)
            logger.info(f"Downloaded {file_key} to {local_path}")
        except ClientError as e:
            logger.error(f"Error downloading file from S3: {e}")
            raise
    
    def upload_file(self, local_path: str, file_key: str, content_type: str = "application/vnd.openxmlformats-officedocument.wordprocessingml.document") -> None:
        try:
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                file_key,
                ExtraArgs={'ContentType': content_type}
            )
            logger.info(f"Uploaded {local_path} to {file_key}")
        except ClientError as e:
            logger.error(f"Error uploading file to S3: {e}")
            raise
    
    def file_exists(self, file_key: str) -> bool:
        try:
            self.s3_client.head_object(Bucket=self.bucket_name, Key=file_key)
            return True
        except ClientError:
            return False


s3_client = S3Client()
