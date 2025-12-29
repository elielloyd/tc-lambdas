import json
import boto3
import requests
import mimetypes
from botocore.exceptions import ClientError

s3 = boto3.client('s3')

def lambda_handler(event, context):
    """
    event:
    {
        "url": "https://example.com/api",
        "bucket": "my-bucket",
        "prefix": "folder/path/",
        "api_access_token": "xyz123"
    }
    """

    url = event.get("url")
    bucket = "trueclaim"
    prefix = event.get("prefix")
    token = event.get("api_access_token")
    conent = event.get("content","")

    if not url or not bucket or not prefix or not token:
        return {
            "statusCode": 400,
            "body": json.dumps("Missing required params: url, bucket, prefix, api_access_token")
        }

    try:
        # Headers including API access token
        headers = {
            "api_access_token": token
        }

        # Fetch S3 objects
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

        if "Contents" not in response:
            return {
                "statusCode": 404,
                "body": json.dumps("No files found in S3 prefix")
            }

        files = response["Contents"]

        # Base multipart form fields (non-file)
        form_fields = [
            ("private", (None, "true")),
            ("content", (None, conent)),

            ("message_type", (None, "outgoing"))
        ]

        # Prepare file attachments
        file_fields = []

        for obj in files:
            key = obj["Key"]

            # skip folder keys
            if key.endswith("/"):
                continue

            file_obj = s3.get_object(Bucket=bucket, Key=key)
            file_bytes = file_obj["Body"].read()

            filename = key.split("/")[-1]

            # detect MIME type
            mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"

            # Add file as attachment[]
            file_fields.append(
                ("attachments[]", (filename, file_bytes, mime_type))
            )

        # Combine all fields
        multipart_payload = form_fields + file_fields

        # Send HTTP POST request
        upload_response = requests.post(
            url,
            files=multipart_payload,
            headers=headers
        )

        return {
            "statusCode": upload_response.status_code,
            "body": upload_response.text
        }

    except ClientError as e:
        return {
            "statusCode": 500,
            "body": json.dumps("S3 Error: " + str(e))
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps("Unexpected Error: " + str(e))
        }
