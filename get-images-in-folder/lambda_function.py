import boto3

def lambda_handler(event, context):
    # Get input parameters
    bucket_name = "trueclaim"
    folder_name = event.get("folder")  # e.g. "my-folder/"
    images_only = event.get("images", False)

    s3 = boto3.client("s3")

    try:
        # List all objects in the folder
        response = s3.list_objects_v2(
            Bucket=bucket_name,
            Prefix=folder_name
        )
        file_names = []
        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj["Key"]
                
                # Skip the folder itself
                if key.endswith("/"):
                    continue

                file_name = key.split("/")[-1]

                if images_only:
                    if file_name.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp")):
                        file_names.append(file_name)
                else:
                    file_names.append(file_name)

        return {
            "files": file_names
        }

    except Exception as e:
        return {
            "files": []
        }
