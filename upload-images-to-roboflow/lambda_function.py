import boto3
import os
import requests
import base64
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

def get_files_from_folder(bucket_name, folder_name):
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
                if file_name.lower().endswith((".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp")):
                    file_info = {
                        "file_name": file_name,
                        "full_path": key
                    }
                    file_names.append(file_info)
        return file_names
    except Exception as e:
        print(f"Error getting files from {folder_name}: {str(e)}")
        return []


def download_file_from_s3(bucket_name, s3_key, local_path):
    """Download a file from S3 to local path"""
    s3 = boto3.client("s3")
    try:
        s3.download_file(bucket_name, s3_key, local_path)
        return True
    except Exception as e:
        print(f"Error downloading {s3_key}: {str(e)}")
        return False



def upload_image_to_roboflow(image_path, api_key, dataset_name, batch_name, split="train", max_retries=3):
    """Upload a single image to Roboflow using the API with retry mechanism"""
    image_name = os.path.basename(image_path)
    error_message = None
    
    # Read and encode image as base64 once
    try:
        with open(image_path, "rb") as image_file:
            image_data = base64.b64encode(image_file.read()).decode("utf-8")
    except Exception as e:
        error_message = f"Error reading file: {str(e)}"
        print(f"Error reading {image_path}: {str(e)}")
        return {"success": False, "file_name": image_name, "error": error_message}
    
    url = f"https://api.roboflow.com/dataset/{dataset_name}/upload"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded"
    }
    
    # Retry logic with exponential backoff
    for attempt in range(max_retries):
        try:
            params = {
                "api_key": api_key,
                "name": image_name,
                "split": split,
                "batch": batch_name
            }
            
            response = requests.post(url, params=params, data=image_data, headers=headers, timeout=30)
            response.raise_for_status()
            result = response.json()
            return {"success": True, "file_name": image_name, "response": result}
            
        except requests.exceptions.RequestException as e:
            error_message = str(e)
            if attempt < max_retries - 1:
                # Exponential backoff: wait 2^attempt seconds
                wait_time = 2 ** attempt
                print(f"Attempt {attempt + 1} failed for {image_name}: {str(e)}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                print(f"All {max_retries} attempts failed for {image_name}: {str(e)}")
                return {"success": False, "file_name": image_name, "error": error_message, "attempts": max_retries}
        except Exception as e:
            error_message = f"Unexpected error: {str(e)}"
            print(f"Unexpected error uploading {image_name}: {str(e)}")
            return {"success": False, "file_name": image_name, "error": error_message}
    
    return {"success": False, "file_name": image_name, "error": error_message or "Unknown error"}


def add_tag_to_image(workspace, project_name, image_id, api_key, tag_value, max_retries=3):
    """Add a tag to an uploaded image in Roboflow"""
    url = f"https://api.roboflow.com/{workspace}/{project_name}/images/{image_id}/tags"
    params = {
        "api_key": api_key
    }
    headers = {
        "Content-Type": "application/json"
    }
    data = {
        "operation": "add",
        "tags": [tag_value]
    }
    
    # Retry logic with exponential backoff
    for attempt in range(max_retries):
        try:
            response = requests.post(url, params=params, json=data, headers=headers, timeout=30)
            response.raise_for_status()
            return {"success": True, "response": response.json()}
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                print(f"Tag attempt {attempt + 1} failed for image {image_id}: {str(e)}. Retrying in {wait_time} seconds...")
                time.sleep(wait_time)
            else:
                error_msg = f"All {max_retries} tag attempts failed: {str(e)}"
                print(error_msg)
                return {"success": False, "error": error_msg}
        except Exception as e:
            error_msg = f"Unexpected error adding tag: {str(e)}"
            print(error_msg)
            return {"success": False, "error": error_msg}
    
    return {"success": False, "error": "Unknown error adding tag"}


def upload_dataset_to_roboflow(dataset_path, api_key, dataset_name, batch_name, workspace, project_name, uuid, num_workers=10):
    """Upload all images in a directory to Roboflow using parallel threads"""
    uploaded_files = []
    failed_files = []
    
    if not os.path.exists(dataset_path):
        error_msg = f"Dataset path {dataset_path} does not exist"
        print(error_msg)
        return {"uploaded": uploaded_files, "failed": failed_files, "error": error_msg}
    
    # Get all image files in the directory
    image_extensions = (".png", ".jpg", ".jpeg", ".gif", ".bmp", ".tiff", ".webp")
    image_files = [
        os.path.join(dataset_path, f) 
        for f in os.listdir(dataset_path) 
        if f.lower().endswith(image_extensions)
    ]
    
    print(f"Found {len(image_files)} images to upload using {num_workers} parallel threads")
    
    # Use ThreadPoolExecutor to upload images in parallel
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        # Submit all upload tasks
        future_to_image = {
            executor.submit(
                upload_image_to_roboflow,
                image_path,
                api_key,
                dataset_name,
                batch_name
            ): image_path
            for image_path in image_files
        }
        
        # Process completed uploads as they finish
        for future in as_completed(future_to_image):
            image_path = future_to_image[future]
            try:
                result = future.result()
                if result.get("success"):
                    # Extract image ID from upload response
                    upload_response = result.get("response", {})
                    image_id = upload_response.get("id")
                    
                    # Add tag to the uploaded image
                    tag_result = None
                    if image_id:
                        tag_result = add_tag_to_image(workspace, project_name, image_id, api_key, uuid)
                        if tag_result.get("success"):
                            print(f"Successfully tagged image {image_id} with uuid: {uuid}")
                        else:
                            print(f"Failed to tag image {image_id}: {tag_result.get('error')}")
                    
                    uploaded_files.append({
                        "file_name": result["file_name"],
                        "response": upload_response,
                        "tag_result": tag_result
                    })
                    print(f"Successfully uploaded: {result['file_name']}")
                else:
                    failed_files.append({
                        "file_name": result["file_name"],
                        "error": result.get("error", "Unknown error")
                    })
                    print(f"Failed to upload: {result['file_name']}")
            except Exception as e:
                error_msg = str(e)
                failed_files.append({
                    "file_name": os.path.basename(image_path),
                    "error": error_msg
                })
                print(f"Exception uploading {os.path.basename(image_path)}: {error_msg}")
    
    print(f"Upload complete: {len(uploaded_files)} succeeded, {len(failed_files)} failed")
    return {"uploaded": uploaded_files, "failed": failed_files}


def lambda_handler(event, context):
    """Lambda handler that processes images and returns detailed logs"""
    response_logs = {
        "status": "success",
        "files_found": [],
        "files_downloaded": [],
        "files_download_failed": [],
        "upload_results": {
            "uploaded": [],
            "failed": []
        },
        "summary": {
            "total_files_found": 0,
            "total_files_downloaded": 0,
            "total_files_download_failed": 0,
            "total_files_uploaded": 0,
            "total_files_upload_failed": 0
        }
    }
    
    try:
        # Get input parameters
        bucket_name = "trueclaim"
        uuid = event.get("uuid")  # e.g. "my-folder/"
        # Define directories to check in priority order
        directories = [
            f"claims/{uuid}/est/InputImages/",
            f"claims/{uuid}/pre/images/estimatics/",
            f"claims/{uuid}/pre/images/ea/",
            f"claims/{uuid}/pre/images/coa/",
            f"claims/{uuid}/est/approved/images/"
        ]
        files = []
        seen_file_names = set()
        
        # Find files in S3
        for directory in directories:
            files_in_directory = get_files_from_folder(bucket_name, directory)
            if files_in_directory:
                for file_info in files_in_directory:
                    file_name = file_info["file_name"]
                    # Only add file if we haven't seen this file name before (remove duplicates)
                    if file_name not in seen_file_names:
                        files.append(file_info)
                        seen_file_names.add(file_name)
                        response_logs["files_found"].append({
                            "file_name": file_name,
                            "full_path": file_info["full_path"],
                            "source_directory": directory
                        })
        
        response_logs["summary"]["total_files_found"] = len(files)
        
        # Download all unique images to data_set directory
        data_set_path = "/tmp/data_set"
        os.makedirs(data_set_path, exist_ok=True)
        
        for file_info in files:
            local_file_path = os.path.join(data_set_path, file_info["file_name"])
            if download_file_from_s3(bucket_name, file_info["full_path"], local_file_path):
                response_logs["files_downloaded"].append({
                    "file_name": file_info["file_name"],
                    "full_path": file_info["full_path"],
                    "local_path": local_file_path
                })
            else:
                response_logs["files_download_failed"].append({
                    "file_name": file_info["file_name"],
                    "full_path": file_info["full_path"],
                    "error": "Failed to download from S3"
                })
        
        response_logs["summary"]["total_files_downloaded"] = len(response_logs["files_downloaded"])
        response_logs["summary"]["total_files_download_failed"] = len(response_logs["files_download_failed"])
        
        # Upload images to Roboflow using API
        api_key = os.getenv('ROBOFLOW_API_KEY', '')
        dataset_name = os.getenv('ROBOFLOW_DATASET_NAME', '')  # Project name
        batch_name = os.getenv('ROBOFLOW_BATCH_NAME', '')
        workspace = os.getenv('ROBOFLOW_WORKSPACE', '')
        project_name = dataset_name  # Project name is the same as dataset name
        upload_results = upload_dataset_to_roboflow(data_set_path, api_key, dataset_name, batch_name, workspace, project_name, uuid)
        
        if upload_results:
            response_logs["upload_results"]["uploaded"] = upload_results.get("uploaded", [])
            response_logs["upload_results"]["failed"] = upload_results.get("failed", [])
            response_logs["summary"]["total_files_uploaded"] = len(response_logs["upload_results"]["uploaded"])
            response_logs["summary"]["total_files_upload_failed"] = len(response_logs["upload_results"]["failed"])
        
        # Set status based on results
        if response_logs["summary"]["total_files_upload_failed"] > 0:
            response_logs["status"] = "partial_success"
        if response_logs["summary"]["total_files_uploaded"] == 0 and response_logs["summary"]["total_files_found"] > 0:
            response_logs["status"] = "failed"
        
    except Exception as e:
        response_logs["status"] = "error"
        response_logs["error"] = str(e)
        print(f"Lambda handler error: {str(e)}")
    
    return response_logs
