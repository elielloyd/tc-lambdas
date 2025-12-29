import os
import hashlib
import base64
import time
import requests
import uuid
import boto3
import json

s3 = boto3.client('s3')
BUCKET_NAME = os.getenv("BUCKET_NAME")


def download_file_from_s3(key, local_path):
    s3.download_file(BUCKET_NAME, key, local_path)


def generate_nonce():
    return str(uuid.uuid4())


def generate_secret_digest(nonce, timestamp, shared_secret):
    secret_string = f"{nonce}{timestamp}{shared_secret}"
    sha1_hash = hashlib.sha1(secret_string.encode('utf-8')).digest()
    base64_encoded = base64.b64encode(sha1_hash).decode('utf-8')
    return base64_encoded


def get_vin_data(vin):
    company_name = os.getenv("CHROMEDATA_COMPANY_NAME")
    realm = os.getenv("CHROMEDATA_REALM")
    app_id = os.getenv("CHROMEDATA_APP_ID")
    shared_secret = os.getenv("CHROMEDATA_SHARED_SECRET")
    digest_method = os.getenv("CHROMEDATA_DIGEST_METHOD")
    company_prefix = os.getenv("CHROMEDATA_COMPANY_PREFIX")

    nonce = generate_nonce()
    timestamp = int(time.time() * 1000)
    secret_digest = generate_secret_digest(nonce, timestamp, shared_secret)

    authorization_header = (
        f'{company_name} '
        f'realm="{realm}", '
        f'{company_prefix}app_id="{app_id}", '
        f'{company_prefix}nonce="{nonce}", '
        f'{company_prefix}secret_digest="{secret_digest}", '
        f'{company_prefix}version="1.0", '
        f'{company_prefix}digest_method="{digest_method}", '
        f'{company_prefix}timestamp="{timestamp}"'
    )

    url = f"https://cvd.api.chromedata.com:443/v1.0/CVD/vin/{vin}?profileKey=CVDPremiumPlus&language_Locale=en_US&vinWithAllContent=true"
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json',
        'Authorization': authorization_header
    }

    response = requests.get(url, headers=headers)

    try:
        data = response.json()
    except ValueError:
        raise ValueError("Invalid JSON response")

    if (
        'result' in data and
        data['result'].get('year') and
        data['result'].get('make') and
        data['result'].get('model')
    ):
        return data

    raise Exception("VIN data not found")


def upload_to_s3(data, key):
    json_data = json.dumps(data, indent=2)
    s3.put_object(Bucket=BUCKET_NAME, Key=key, Body=json_data, ContentType='application/json')


def lambda_handler(event, context):
    vin = event.get('vin')
    claim_id = event.get('claim_id')

    if not vin:
        raise Exception("VIN is required but not provided in the event payload.")

    vin_decode_result = get_vin_data(vin)
    if claim_id:
        # Define S3 path
        s3_vin_decode_path = f"claims/{claim_id}/pre/jdpower.json"

        # Upload to S3
        upload_to_s3(vin_decode_result, s3_vin_decode_path)

    return vin_decode_result

