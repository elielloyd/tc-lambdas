import boto3
import json
import os
from openai_executions import get_pois_for_batch
import base64

s3 = boto3.client('s3')
BUCKET_NAME = os.getenv("BUCKET_NAME")
def download_file_from_s3(key, local_path):
    s3.download_file(BUCKET_NAME, key, local_path)


def list_s3_files(prefix):
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    keys = []
    if "Contents" in response:
        for obj in response["Contents"]:
            keys.append(obj["Key"])
    return keys

def get_and_download_input_images(claim_id):
    base_est_prefix = f"claims/{claim_id}/est/"
    keys = list_s3_files(base_est_prefix + "InputImages/")
    local_orig_images= []
    for key in keys:
        print("keys",key)
        local_file = os.path.join("/tmp",key)
        os.makedirs(os.path.dirname(local_file), exist_ok=True)
        download_file_from_s3(key, local_file)
        local_orig_images.append(local_file)
    return local_orig_images

# --- Encode one image with base64 and filename ---
def encode_image_with_name(image_path):
    with open(image_path, "rb") as f:
        base64_img = base64.b64encode(f.read()).decode("utf-8")
        return (
            os.path.basename(image_path),
            {
                "type": "image_url",
                "image_url": {
                    "url": f"data:image/jpeg;base64,{base64_img}",
                }
            }
        )

# --- Merge multiple batch results into one ---
def merge_poi_mappings(mappings):
    combined = {}
    for mapping in mappings:
        for poi, imgs in mapping.items():
            if poi not in combined:
                combined[poi] = set()
            combined[poi].update(imgs)
    return {poi: list(imgs) for poi, imgs in combined.items()}


# --- Process image list in batches ---
def process_images_with_user_description(
    prompt,
    image_paths,
    batch_size = 5
):
    all_results = []
    for i in range(0, len(image_paths), batch_size):
        batch_paths = image_paths[i:i + batch_size]
        # Encode images and extract filenames + OpenAI dicts
        image_metadata = [encode_image_with_name(p) for p in batch_paths]
        filenames = [name for name, _ in image_metadata]
        image_inputs = [img for _, img in image_metadata]
        print(f"Processing batch {i//batch_size + 1}/{(len(image_paths) + batch_size - 1) // batch_size}", image_inputs)
        # Call GPT for this batch
        batch_result = get_pois_for_batch(prompt, filenames, image_inputs)
        all_results.append(batch_result)
    return merge_poi_mappings(all_results)