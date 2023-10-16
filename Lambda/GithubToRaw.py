import zipfile
import os
import tempfile
import urllib3
import boto3
import shutil
from botocore.exceptions import NoCredentialsError

def upload_files_to_s3(local_directory, bucket_name, s3_prefix=''):
    s3 = boto3.client('s3')
    for root, dirs, files in os.walk(local_directory):
        for file in files:
            local_path = os.path.join(root, file)
            relative_path = os.path.relpath(local_path, local_directory)
            s3_path = os.path.join(s3_prefix, relative_path)
            try:
                s3.upload_file(local_path, bucket_name, s3_path)
            except NoCredentialsError:
                print("AWS credentials not available.")
                return

def lambda_handler(event, context):
    # GitHub repository details
    github_repo_url = "https://github.com/squareshift/stock_analysis/archive/refs/heads/main.zip"
    
    # S3 bucket details
    s3_bucket_name = "stock-analysis-praghadeesh"
    s3_object_prefix = "raw"  # Optional: prefix to add to S3 object keys
    
    # Create a temporary directory to store the downloaded zip file
    temp_dir = tempfile.mkdtemp()

    try:
        # Initialize urllib3 PoolManager
        http = urllib3.PoolManager()

        # Download the zip file from GitHub
        response = http.request('GET', github_repo_url)
        if response.status == 200:
            zip_file_path = os.path.join(temp_dir, "repo.zip")
            with open(zip_file_path, "wb") as f:
                f.write(response.data)

            # Extract the files without creating a new zip
            with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
                zip_ref.extractall(temp_dir)
            upload_files_to_s3(temp_dir, s3_bucket_name, s3_object_prefix)

            return {
                "statusCode": 200,
                "body": "Data ingested and stored in S3"
            }

        else:
            return {
                "statusCode": response.status,
                "body": "Failed to download the GitHub repository"
            }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": str(e)
        }
    finally:
        # Clean up the temporary directory and its contents using shutil
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
