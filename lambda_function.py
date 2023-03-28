import boto3
import io

def merge_s3_files(bucket_name, merged_file_name):
    # Create an S3 client
    s3 = boto3.client('s3')

    # Create a buffer to store the merged file
    buffer = io.BytesIO()

    # List all objects in the S3 bucket
    list_objects_response = s3.list_objects_v2(Bucket=bucket_name)

    # Loop through all objects in the S3 bucket
    for object in list_objects_response['Contents']:
        # Read the contents of the object
        object_response = s3.get_object(Bucket=bucket_name, Key=object['Key'])
        object_contents = object_response['Body'].read()

        # Append the object contents to the buffer
        buffer.write(object_contents)

        # Delete the object from the S3 bucket
        s3.delete_object(Bucket=bucket_name, Key=object['Key'])

    # Write the buffer to a new object in the S3 bucket
    buffer.seek(0)
    s3.put_object(Body=buffer, Bucket=bucket_name, Key=merged_file_name)


bucket_name = 'deaws-bikeshare'
merged_file_name = 'bikeshare_merged.csv'
merge_s3_files(bucket_name, merged_file_name)
