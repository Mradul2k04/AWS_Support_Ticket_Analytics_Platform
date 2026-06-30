from pyspark.sql.types import *
from pyspark.sql import SparkSession
from config.config import BUCKET_NAME
from utils.logger import logger
from pyspark.sql import functions as func
import os
S3_RAW_BUCKET = f"s3a://{BUCKET_NAME}/raw/"

    
def create_spark_session():
    #Set HADOOP_HOME for Windows native IO 
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

    return (
       SparkSession.builder
        .appName("Support Ticket Analytics")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.3.4,"
            "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
            "org.postgresql:postgresql:42.6.0"
        )
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.local.dir",            "C:/spark-tmp")
        # ── Memory 
        .config("spark.driver.memory","6g")   
        .config("spark.executor.memory","6g")   
        .config("spark.memory.fraction","0.8")
        # ── Timeouts 
        .config("spark.executor.heartbeatInterval", "120s")
        .config("spark.network.timeout","800s")
        # ── S3 speed 
        .config("spark.hadoop.fs.s3a.fast.upload","true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.multipart.size", "67108864")
        # ── Partitions reduce 
        .config("spark.sql.shuffle.partitions", "4")   # ← default 200 se 4
        .config("spark.default.parallelism","4")
        .config(
            "spark.driver.extraJavaOptions",
            "-Djava.net.preferIPv4Stack=true "
            "-Djava.io.tmpdir=C:/spark-tmp "
            "-Dlog4j.configurationFile=log4j2.properties"
        )
        .config(
            "spark.executor.extraJavaOptions",
            "-Djava.net.preferIPv4Stack=true "
            "-Djava.io.tmpdir=C:/spark-tmp"
        )
        .getOrCreate()
    )


def extract(spark):
    """
        PySpark Task 1 — read both source files with explicit schemas
        and correct timestamp format.
    """

   # Read with header only — let Spark use actual CSV column names
    issues_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(f"{S3_RAW_BUCKET}issues.csv")
    )

    # Select and cast ONLY the columns we need — this IS the explicit schema
    issues_df = issues_raw.select(
        func.col("id").cast(LongType()),
        func.col("issue_num").cast(LongType()),
        func.col("issue_proj").cast(StringType()),
        func.col("issue_reporter").cast(StringType()),
        func.col("issue_assignee").cast(StringType()),
        func.col("issue_contr_count").cast(IntegerType()),
        func.col("issue_priority").cast(StringType()),
        func.col("issue_type").cast(StringType()),
        func.col("issue_created").cast(StringType()),        # cast in transform
        func.col("issue_resolution_date").cast(StringType()),# cast in transform
        func.col("issue_resolution").cast(StringType()),
        func.col("issue_status").cast(StringType()),
        func.col("issue_comments_count").cast(IntegerType()),
        func.col("last_change_date").cast(StringType()),     # cast in transform
        func.col("wf_total_time").cast(LongType()),
        func.col("processing_steps").cast(IntegerType()),
    )

    # Change history — same approach
    change_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(f"{S3_RAW_BUCKET}issues_change_history.csv")
    )

    change_df = change_raw.select(
        func.col("id").cast(LongType()),
        func.col("issueid").cast(LongType()),
        func.col("field").cast(StringType()),
        func.col("value").cast(StringType()),
        func.col("created").cast(StringType()),              # cast in transform
        func.col("change_group_id").cast(LongType()),
    )

    issues_count = issues_df.count()
    change_count = change_df.count()
    logger.info(f"Issues count: {issues_count}")
    logger.info(f"History count: {change_count}")

    return issues_df, change_df



if __name__ == "__main__":
    spark = create_spark_session()
    issues_df , change_df =extract(spark)
    issues_df.printSchema()
    issues_df.show(5,truncate=False)
    change_df.printSchema()
    change_df.show(5,truncate=False)