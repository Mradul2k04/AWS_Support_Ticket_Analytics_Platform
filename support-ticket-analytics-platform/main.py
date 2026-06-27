from glob import glob
import shutil

from utils.logger import logger
from aws.s3_bucket.create_bucket import create_bucket
from aws.s3_bucket.create_folder import create_folder
from aws.s3_bucket.upload_file import upload_file
from config.config import RAW_PREFIX
from pyspark_folder.extraction.extract import extract,create_spark_session
from pyspark.sql import SparkSession
from pyspark_folder.transformation.transformation import transform



def main():
    
    #CREATING A S3 BUCKET USING BOTO3 SCRIPTS
    # create_bucket()

    #CREATING FOLDERS INSIDE S3 BUCKET (raw/ , curated/ )
    # create_folder()

    #UPLOADING FILES INSIDE S3 BUCKET FOLDERS
    # upload_file("datasets/issues.csv", RAW_PREFIX + "issues.csv")
    # upload_file("datasets/issues_change_history.csv",RAW_PREFIX + "issues_change_history.csv")

    logger.info("Pipeline Started....")
    # Step 1 — Create Spark session
    spark = create_spark_session()
    hadoop_conf = spark._jsc.hadoopConfiguration()

    hadoop_conf.set(
    "fs.s3a.aws.credentials.provider",
    "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    )
    # Step 2 — Extract
    logger.info("Starting extraction...")
    issues_df, change_df =extract(spark)

    # Step 3 — Transform
    logger.info("Starting transformation...")
    fact_df, agent_agg_df, resolution_perf_df, cat_trends_df = transform(issues_df, change_df)
    spark.stop()


   
if __name__=="__main__":
    main()