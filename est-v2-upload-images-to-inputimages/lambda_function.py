import boto3
import os

s3 = boto3.client('s3')

def lambda_handler(event, context):
    tc_claim_id = event['tcClaimId']
    bucket = 'trueclaim'
    base_path = f'claims/{tc_claim_id}'
    input_images_path = f'{base_path}/est/InputImages'

    folders_to_check = ['pre/images/estimatics', 'pre/images/coa', 'pre/images/ea']
    selected_folder = None

    # Check folders in priority order
    for folder in folders_to_check:
        prefix = f'{base_path}/{folder}/'
        response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=1)
        if 'Contents' in response and response['Contents']:
            selected_folder = folder
            break

    if not selected_folder:
        raise Exception(f"No validalid folders")

    print(f"Selected folder: {selected_folder}")

    # List all files in selected folder
    full_prefix = f'{base_path}/{selected_folder}/'
    response = s3.list_objects_v2(Bucket=bucket, Prefix=full_prefix)

    if 'Contents' not in response:
        raise Exception(f"No files found in {full_prefix}")

    for obj in response['Contents']:
        source_key = obj['Key']
        file_name = os.path.basename(source_key)
        destination_key = f'{input_images_path}/{file_name}'

        copy_source = {
            'Bucket': bucket,
            'Key': source_key
        }

        # Copy the file within S3
        s3.copy_object(
            Bucket=bucket,
            CopySource=copy_source,
            Key=destination_key
        )

        print(f"Copied {source_key} to {destination_key}")

    return {
        'statusCode': 200,
        'body': f'Copied files from {selected_folder} to InputImages.'
    }
