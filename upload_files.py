import boto3
from botocore.exceptions import ClientError
import pathlib


# Define bucket name and region
bucket_name = 'deaws-bikeshare'
region = 'eu-west-2'

# Create an S3 client
s3 = boto3.client('s3', region_name=region)

# Create the bucket in the defined region
try:
    s3.create_bucket(Bucket=bucket_name, 
                     CreateBucketConfiguration={
                     'LocationConstraint': region
                 })
    print("Bucket created successfully")
except ClientError as e:
    if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
        print("Bucket already exists and is owned by you, ignoring creation")
    
# Define file path and upload the file to S3
bikeshare_folder = pathlib.Path("../bikeshare")
print(bikeshare_folder)
files = [str(file) for file in bikeshare_folder.glob('*')]
print(files)
for file_path in files:
    s3.upload_file(file_path, bucket_name, f"raw_{file_path.split('/')[-1]}")



