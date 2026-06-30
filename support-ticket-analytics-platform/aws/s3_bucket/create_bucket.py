from config.config import BUCKET_NAME, AWS_REGION
from botocore.exceptions import ClientError
from aws.s3_bucket.s3_helper import s3_client
def create_bucket():
    try:
        s3_client.create_bucket(
            Bucket=BUCKET_NAME,
            CreateBucketConfiguration={
                'LocationConstraint': AWS_REGION
            }
        )
        print(f"Bucket Created : {BUCKET_NAME}")

    except ClientError as e:
        print(e)

if __name__ == "__main__":
    create_bucket()