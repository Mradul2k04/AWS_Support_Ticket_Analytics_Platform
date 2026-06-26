from pyspark.sql.types import *
from pyspark.sql import SparkSession
from config.config import BUCKET_NAME
from utils.logger import logger
from config.config import ISSUES_FILE, HISTORY_FILE
S3_RAW_BUCKET = f"s3a://{BUCKET_NAME}/raw/"


ISSUES_SCHEMA = StructType([
    StructField("id",                    LongType(),      False),  
    StructField("issue_num",             LongType(),      True),   
    StructField("issue_proj",            StringType(),    True),    
    StructField("issue_reporter",        StringType(),    True),   
    StructField("issue_assignee",        StringType(),    True),    
    StructField("issue_contr_count",     IntegerType(),   True),   
    StructField("issue_priority",        StringType(),    True),   
    StructField("issue_type",            StringType(),    True),   
    StructField("issue_created",         TimestampType(), True),   
    StructField("issue_resolution_date", TimestampType(), True),   
    StructField("issue_resolution",      StringType(),    True),   
    StructField("issue_status",          StringType(),    True),   
    StructField("issue_comments_count",  IntegerType(),   True),
    StructField("last_change_date",      TimestampType(), True),
    StructField("wf_total_time",         LongType(),      True),   
    StructField("processing_steps",      IntegerType(),   True),
])

CHANGE_HISTORY_SCHEMA = StructType([
    StructField("id",              LongType(),      False),  # change record unique ID
    StructField("issueid",         LongType(),      False),  # FK → issues.id
    StructField("field",           StringType(),    True),   # "assignee" or "status"
    StructField("value",           StringType(),    True),   # new value after change
    StructField("created",         TimestampType(), True),   # when change was made
    StructField("change_group_id", LongType(),      True),
])


def create_spark_session():
    return (
       SparkSession.builder
    .appName("Support Ticket Analytics")
    .config(
        "spark.jars.packages",
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
    )
    .config(
        "spark.hadoop.fs.s3a.impl",
        "org.apache.hadoop.fs.s3a.S3AFileSystem"
    )
    .getOrCreate()
    )


def extract(spark):
    """
        PySpark Task 1 — read both source files with explicit schemas
        and correct timestamp format.
    """

    issues_df = (
        spark.read.option("header",True)
        .option("timestampFormat","yyyy-MM-dd HH:mm:ss")
        .schema(ISSUES_SCHEMA)
        .csv(f"{S3_RAW_BUCKET}issues.csv")
        # .csv(ISSUES_FILE)
    )

    change_df = (
        spark.read.option("header",True)
        .option("timestampFormat","yyyy-MM-dd HH:mm:ss")
        .schema(CHANGE_HISTORY_SCHEMA)
        .csv(f"{S3_RAW_BUCKET}issues_change_history.csv")
        # .csv(HISTORY_FILE)
    )

    #ROW COUNT - LOG IMMEDIATELY AFTER READ
    issues_count = issues_df.count()
    change_count = change_df.count()
    logger.info(f"Issues count: {issues_count}")
    logger.info(f"History count: {change_count}")

    return (
        issues_df, change_df
    )


if __name__ == "__main__":
    spark = create_spark_session()
    issues_df , change_df =extract(spark)
    issues_df.printSchema()
    issues_df.show(5)
    change_df.printSchema()
    change_df.show(5)