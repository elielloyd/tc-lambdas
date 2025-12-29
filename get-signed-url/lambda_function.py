import boto3
import os
import json
from urllib.parse import unquote

s3 = boto3.client(
    "s3",
    region_name="us-east-1",
    aws_access_key_id=os.environ["STATIC_AWS_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["STATIC_AWS_SECRET_ACCESS_KEY"],
)

BUCKET_NAME = os.environ.get("BUCKET_NAME")
URL_EXPIRY = 3600  # 1 hour


def lambda_handler(event, context):
    try:
        folder_path = event.get("path")
        if not folder_path:
            return {"error": "Missing 'path' in request"}

        if not folder_path.endswith("/"):
            folder_path += "/"

        response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=folder_path)
        contents = response.get("Contents", [])

        if not contents:
            return {"path": folder_path, "files": [], "message": "No files found"}

        files = []
        for obj in contents:
            key = obj["Key"]
            if key.endswith("/"):
                continue

            # Generate presigned URL (SigV4 by default)
            url = s3.generate_presigned_url(
                "get_object",
                Params={"Bucket": BUCKET_NAME, "Key": key},
                ExpiresIn=URL_EXPIRY,
            )

            # Decode only the visible path part (before the ?)
            base, query = url.split("?", 1)
            
            decoded_base = unquote(base)
            decoded_url = f"{decoded_base}?{query}"

            files.append(url)

        return {
            "path": folder_path,
            "files": files
        }

    except Exception as e:
        return {"error": str(e)}
