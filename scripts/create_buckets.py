import boto3
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_minio_buckets():
    # Initialisation du client MinIO
    s3_client = boto3.client(
        's3',
        endpoint_url='http://minio:9000',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin'
    )

    # Liste des buckets à créer
    buckets = ['raw-videos', 'metadata', 'processed']

    for bucket in buckets:
        try:
            s3_client.create_bucket(Bucket=bucket)
            logger.info(f"Bucket '{bucket}' créé avec succès")
        except s3_client.exceptions.BucketAlreadyOwnedByYou:
            logger.info(f"Bucket '{bucket}' existe déjà")
        except Exception as e:
            logger.error(f"Erreur lors de la création du bucket '{bucket}' : {e}")
            raise

if __name__ == "__main__":
    create_minio_buckets()  