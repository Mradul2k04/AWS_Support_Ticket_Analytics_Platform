from aws.s3_bucket.s3_helper import s3_client
from config.config import BUCKET_NAME, RAW_PREFIX

def upload_file(file, s3_key):
    try:
        s3_client.upload_file(file, BUCKET_NAME, s3_key)
        print(f"Uploaded {file}")
    except Exception as e:
        print(e)


if __name__ == "__main__":
    upload_file("datasets/issues.csv", RAW_PREFIX+"issues.csv")

