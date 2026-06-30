import os
from dotenv import load_dotenv

load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("BUCKET_NAME")

RAW_PREFIX = os.getenv("RAW_PREFIX")
CURATED_PREFIX = os.getenv("CURATED_PREFIX")

ISSUES_FILE = os.getenv("ISSUES_FILE")
HISTORY_FILE = os.getenv("HISTORY_FILE")

RDS_HOST = os.getenv("RDS_HOST")
RDS_PORT = os.getenv("RDS_PORT")
RDS_DB = os.getenv("RDS_DB")
RDS_PASS = os.getenv("RDS_PASS")
RDS_USER = os.getenv("RDS_USER")