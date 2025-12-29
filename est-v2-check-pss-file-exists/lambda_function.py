import boto3
import json

s3 = boto3.client("s3")
BUCKET = "trueclaim"
def lambda_handler(event, context):
    key = event['key']

    if not key:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "bucket and key are required"})
        }

    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        # If no exception, file exists
        return {
            "exists": True
        }
    except s3.exceptions.ClientError as e:
        return {"exists": False
            }