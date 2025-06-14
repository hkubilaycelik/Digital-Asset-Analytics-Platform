import boto3
import os

# --- Configuration ---
BUCKET_NAME = "digital-asset-analytics-data-lake" 
LOCAL_FILE_PATH = "/Users/hkubilayc/Documents/GitHub/Digital-Asset-Analytics-Platform/Data/BTCUSDT-trades-2025-05.csv"

S3_OBJECT_KEY = f"raw/{os.path.basename(LOCAL_FILE_PATH)}"


def upload_file_to_s3():
    """
    Uploads the to S3 bucket
    """
    # Creates a client to interact with the S3 service.
    # Uses the credentials you configured with `aws configure`.
    s3_client = boto3.client('s3')

    print(f"Starting upload of {LOCAL_FILE_PATH} to s3://{BUCKET_NAME}/{S3_OBJECT_KEY}")
    print("...")
    print("This will take a long time and the script will show no progress until it is finished.")

    try:
        s3_client.upload_file(LOCAL_FILE_PATH, BUCKET_NAME, S3_OBJECT_KEY)
        print("\nSUCCESS: Upload Complete!")

    except FileNotFoundError:
        print(f"ERROR: The file was not found at {LOCAL_FILE_PATH}")
    except Exception as e:
        print(f"\nAn error occurred: {e}")


# --- Run the Script ---
if __name__ == "__main__":
    
    if BUCKET_NAME == "digital-asset-analytics-data-lake" and LOCAL_FILE_PATH == "/Users/hkubilayc/Documents/GitHub/Digital-Asset-Analytics-Platform/Data/BTCUSDT-trades-2025-05.csv":
        upload_file_to_s3()
