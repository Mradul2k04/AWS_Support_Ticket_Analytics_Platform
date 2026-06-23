from aws.s3_bucket.s3_helper import s3_client
from config.config import BUCKET_NAME,RAW_PREFIX,CURATED_PREFIX

def create_folder():
    folders =[
        RAW_PREFIX,CURATED_PREFIX
    ]

    for folder in folders:
        s3_client.put_object(
            Bucket = BUCKET_NAME,
            Key = folder
        )
    print("Folder Structure Created")

if __name__ == "__main__":
    create_folder()