from aws.s3_bucket.create_bucket import create_bucket
from aws.s3_bucket.create_folder import create_folder
from aws.s3_bucket.upload_file import upload_file
from config.config import RAW_PREFIX
from pyspark_folder.extraction.extract import extract,create_spark_session
from pyspark.sql import SparkSession

def main():
    
    #CREATING A S3 BUCKET USING BOTO3 SCRIPTS
    # create_bucket()

    #CREATING FOLDERS INSIDE S3 BUCKET (raw/ , curated/ )
    # create_folder()

    #UPLOADING FILES INSIDE S3 BUCKET FOLDERS
    # upload_file("datasets/issues.csv", RAW_PREFIX + "issues.csv")
    # upload_file("datasets/issues_change_history.csv",RAW_PREFIX + "issues_change_history.csv")

    spark = create_spark_session()
    hadoop_conf = spark._jsc.hadoopConfiguration()

    hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    extract(spark)
if __name__=="__main__":
    main()