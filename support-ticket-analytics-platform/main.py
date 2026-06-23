from aws.s3_bucket.create_bucket import create_bucket
from aws.s3_bucket.create_folder import create_folder
from aws.s3_bucket.upload_file import upload_file
from config.config import RAW_PREFIX

def main():
    
    #CREATING A S3 BUCKET USING BOTO3 SCRIPTS
    # create_bucket()

    #CREATING FOLDERS INSIDE S3 BUCKET (raw/ , curated/ )
    # create_folder()

    #UPLOADING FILES INSIDE S3 BUCKET FOLDERS
    # upload_file("datasets/issues.csv", RAW_PREFIX + "issues.csv")
    # upload_file("datasets/issues_change_history.csv",RAW_PREFIX + "issues_change_history.csv")

if __name__=="__main__":
    main()