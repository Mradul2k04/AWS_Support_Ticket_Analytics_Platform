from pyspark.sql.types import *
from pyspark.sql import SparkSession
from config.config import BUCKET_NAME
from utils.logger import logger
from pyspark.sql import functions as func
import os
import boto3
from config.config import ISSUES_FILE, HISTORY_FILE
S3_RAW_BUCKET = f"s3a://{BUCKET_NAME}/raw/"


def create_spark_session():
    # Set HADOOP_HOME for Windows native IO
    os.environ["HADOOP_HOME"] = "C:\\hadoop"
    os.environ["PATH"] = "C:\\hadoop\\bin;" + os.environ.get("PATH", "")

    # Resolve credentials the same way boto3 already successfully does
    session = boto3.Session()
    creds = session.get_credentials().get_frozen_credentials()

    spark = (
        SparkSession.builder
        .appName("Support Ticket Analytics")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.2,"
            "org.postgresql:postgresql:42.6.0"
        )
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.access.key", creds.access_key)
        .config("spark.hadoop.fs.s3a.secret.key", creds.secret_key)
        .config("spark.local.dir",            "C:/spark-tmp")
        # ── Memory
        .config("spark.driver.memory",        "6g")
        .config("spark.executor.memory",      "6g")
        .config("spark.memory.fraction",      "0.8")
        # ── Timeouts
        .config("spark.executor.heartbeatInterval", "120s")
        .config("spark.network.timeout",            "800s")
        # ── S3 speed
        .config("spark.hadoop.fs.s3a.fast.upload",        "true")
        .config("spark.hadoop.fs.s3a.fast.upload.buffer", "bytebuffer")
        .config("spark.hadoop.fs.s3a.multipart.size",     "67108864")
        # ── Partitions reduce
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism",    "4")
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

    if creds.token:
        spark._jsc.hadoopConfiguration().set("fs.s3a.session.token", creds.token)
        spark._jsc.hadoopConfiguration().set(
            "fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider"
        )

    return spark


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
    # Numeric columns go through DoubleType first, since the CSV stores
    # some integer-like values as floats (e.g. "24665.0"), which Spark 4's
    # ANSI-mode cast rejects when going straight to Long/Integer.
    issues_df = issues_raw.select(
        func.col("id").cast(DoubleType()).cast(LongType()),
        func.col("issue_num").cast(DoubleType()).cast(LongType()),
        func.col("issue_proj").cast(StringType()),
        func.col("issue_reporter").cast(StringType()),
        func.col("issue_assignee").cast(StringType()),
        func.col("issue_contr_count").cast(DoubleType()).cast(IntegerType()),
        func.col("issue_priority").cast(StringType()),
        func.col("issue_type").cast(StringType()),
        func.col("issue_created").cast(StringType()),        # cast in transform
        func.col("issue_resolution_date").cast(StringType()),# cast in transform
        func.col("issue_resolution").cast(StringType()),
        func.col("issue_status").cast(StringType()),
        func.col("issue_comments_count").cast(DoubleType()).cast(IntegerType()),
        func.col("last_change_date").cast(StringType()),     # cast in transform
        func.col("wf_total_time").cast(DoubleType()).cast(LongType()),
        func.col("processing_steps").cast(DoubleType()).cast(IntegerType()),
    )

    # Change history — same approach
    change_raw = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "false")
        .csv(f"{S3_RAW_BUCKET}issues_change_history.csv")
    )

    change_df = change_raw.select(
        func.col("id").cast(DoubleType()).cast(LongType()),
        func.col("issueid").cast(DoubleType()).cast(LongType()),
        func.col("field").cast(StringType()),
        func.col("value").cast(StringType()),
        func.col("created").cast(StringType()),              # cast in transform
        func.col("change_group_id").cast(DoubleType()).cast(LongType()),
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