import boto3 
from config.config import AWS_REGION

# #OPEN MANAGEMENT CONSOLE
# aws_management_console = boto3.session.Session(profile_name="default")
# # print(aws_management_console.region_name)
# #OPEN IAM CONSOLE


#No, the bucket and file will be uploaded to your AWS account only if Boto3 is already authenticated.
s3_client = boto3.client("s3", region_name = AWS_REGION) 