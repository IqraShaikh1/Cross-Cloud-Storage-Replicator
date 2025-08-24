import os
from flask import Flask, request, jsonify
from dotenv import load_dotenv
import boto3
from botocore.exceptions import ClientError
from google.cloud import storage

load_dotenv()

app = Flask(__name__)

# Getting GCS bucket name
GCS_BUCKET_NAME = os.getenv("GCS_BUCKET_NAME")

#Check that GCS bucket is valid or not
if not GCS_BUCKET_NAME:
    raise ValueError("GCS_BUCKET_NAME environment variable not set. Please configure it in your .env file.")

def gcs_blob_exists(bucket_name, blob_name):
    """
    Checks if a file (blob) already exists in a Google Cloud Storage bucket for the idempotency check.
    Passing bucket name,file name to check
    """
    try:
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        return blob.exists()
    except Exception as e:
        print(f"Error: Failed to check GCS blob existence for gs://{bucket_name}/{blob_name}. Reason: {e}")
        return False

def stream_s3_to_gcs(s3_bucket, s3_key, gcs_bucket_name):
    """
    Streaming a file directly from an S3 bucket to a GCS bucket.
    Passing s3_bucket ,s3_key,gcs_bucket_name 

    """
    try:
        s3_client = boto3.client('s3')
        storage_client = storage.Client()
        s3_object = s3_client.get_object(Bucket=s3_bucket, Key=s3_key)
        s3_stream = s3_object['Body']
        gcs_bucket = storage_client.bucket(gcs_bucket_name)
        blob = gcs_bucket.blob(s3_key)
        print(f"Streaming s3://{s3_bucket}/{s3_key} to gs://{gcs_bucket_name}/{s3_key}...")
        blob.upload_from_file(s3_stream, content_type=s3_object.get("ContentType", "application/octet-stream"))
        print("Stream successful.")
    
        return True, None

    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchKey':
            error_message = f"File not found in S3: s3://{s3_bucket}/{s3_key}"
            print(error_message)
            return False, (error_message, 404)
        else:
            error_message = f"An S3 client error occurred: {e}"
            print(error_message)
            return False, (error_message, 500)
    except Exception as e:
        error_message = f"An unexpected error occurred during the stream: {e}"
        print(error_message)
        return False, (error_message, 500)

# --- API Endpoint ---

@app.route("/v1/replicate", methods=["POST"])
def replicate_file():
    """
    The main API endpoint to handle the replication request.
    """
    # 1. Validation of incoming request payload
    data = request.get_json()
    if not data or 's3_bucket' not in data or 's3_key' not in data:
        return jsonify({"error": "Invalid request. JSON payload must include 's3_bucket' and 's3_key'."}), 400

    s3_bucket = data['s3_bucket']
    s3_key = data['s3_key']
    print(f"Received request to replicate s3://{s3_bucket}/{s3_key}")

    # 2. Idempotency Check: Don't re-upload if the file already exists in the destination.
    if gcs_blob_exists(GCS_BUCKET_NAME, s3_key):
        print(f"Idempotency check passed: File gs://{GCS_BUCKET_NAME}/{s3_key} already exists. Skipping.")
        return jsonify({
            "message": "File already exists in the destination. Replication skipped.",
            "status": "skipped"
        }), 200 

    # 3. Performing the stream replication
    success, error = stream_s3_to_gcs(s3_bucket, s3_key, GCS_BUCKET_NAME)

    if not success:
        error_message, status_code = error
        return jsonify({"error": error_message}), status_code

    # 4. Success
    return jsonify({
        "message": "File replicated successfully.",
        "source": f"s3://{s3_bucket}/{s3_key}",
        "destination": f"gs://{GCS_BUCKET_NAME}/{s3_key}"
    }), 201 


if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
