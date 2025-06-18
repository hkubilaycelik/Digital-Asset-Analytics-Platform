import boto3
import os

# --- Configuration ---
BUCKET_NAME = "digital-asset-analytics-data-lake" 
LOCAL_FILE_PATH = "/Users/hkubilayc/Documents/GitHub/Digital-Asset-Analytics-Platform/Data/BTCUSDT-trades-2025-05.csv"

S3_OBJECT_KEY = f"raw/{os.path.basename(LOCAL_FILE_PATH)}"


def upload_file_to_s3(bucket_name, local_file_path, s3_object_key):
    """
    Uploads a local file to an S3 bucket

    :param bucket_name: Name of the target S3 bucket.
    :param local_file_path: Path to the local file to upload.
    :param s3_object_key: The desired key (path) for the object in S3.
    """
    # Create a client to interact with the S3 service.
    s3_client = boto3.client('s3')

    print(f"Starting upload of {local_file_path} to s3://{bucket_name}/{s3_object_key}")
    print("...")
    print("This may take a long time and the script will show no progress until it is finished.")

    try:
        s3_client.upload_file(local_file_path, bucket_name, s3_object_key)
        print("\nSUCCESS: Upload Complete!")

    except FileNotFoundError:
        print(f"ERROR: The file was not found at {local_file_path}")
    except Exception as e:
        print(f"\nAn error occurred: {e}")


# --- Run the Script ---
if __name__ == "__main__":
    
    if BUCKET_NAME == "digital-asset-analytics-data-lake" and LOCAL_FILE_PATH == "/Users/hkubilayc/Documents/GitHub/Digital-Asset-Analytics-Platform/Data/BTCUSDT-trades-2025-05.csv":
        upload_file_to_s3(BUCKET_NAME, LOCAL_FILE_PATH, S3_OBJECT_KEY)
