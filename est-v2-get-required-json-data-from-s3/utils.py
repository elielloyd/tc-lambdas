import boto3
import json
import os

s3 = boto3.client('s3')

AVAILABLE_OPERATIONS = {
  1:"Remove/Replace",
  2: "Remove/Install",
  3:"Additional Labor",
  4:"Align",
  5:"Overhaul",
  6:"Refinish Only",
  7:"Access/Inspect",
  8:"Check/Adjust",
  9:"Repair",
  10:"Blend",
  16:"Paintless Repair"
}

def get_s3_file(key, default_value=None):
    try:
        bucket = os.getenv("BUCKET_NAME")
        response = s3.get_object(Bucket=bucket, Key=key)
        return response['Body'].read().decode('utf-8')
    except Exception as e:
        print(f"Error fetching file from S3: {e}")
        raise Exception(f"Error fetching file from S3: {e} : {key}")



def get_damage_description_v2(claim_id,mitchell_json):
    driver_txt_key = f"claims/{claim_id}/est/ResultCsvs/driver_description.txt"
    # Try to get driver_description.txt
    try:
        txt_file_content = get_s3_file(driver_txt_key)
    except:
        txt_file_content = ''
    if txt_file_content:
        try:
            damage_description = txt_file_content.strip()
            if damage_description:
                return damage_description
        except Exception as e:
            print(f"Failed to decode driver_description.txt: {e}")

    print("Driver description not found or empty, checking mitchell.json")
    # Try to get mitchell.json
    if mitchell_json:
        try:
            return mitchell_json["claimInfo"]['overview']["vehicle"]["status"]["damageDescription"]
        except json.JSONDecodeError:
            print("Error decoding mitchell.json")
        except KeyError as e:
            print(f"Missing key in mitchell.json: {e}")
        except Exception as e:
            print(f"Error processing mitchell.json: {e}")
    return None

def extract_required_pss_data(full_pss_data):
    """
    Extract only required fields from PSS data
    """
    optimized_pss = {"Categories": [] ,'SuperCategories':full_pss_data.get('SuperCategories',[])}
    categories = full_pss_data.get("Categories", [])
    for category in categories:
        optimized_category = {
            "Id": category.get("Id"),
            "Description": category.get("Description"),
            "SubCategories": []
        }
        
        subcategories = category.get("SubCategories", [])
        for subcategory in subcategories:
            optimized_subcategory = {
                "Id": subcategory.get("Id"),
                "Description": subcategory.get("Description"),
                "Parts": [],
                "Images": extract_images(subcategory.get("Images", []))
            }
            
            parts = subcategory.get("Parts", [])
            for part in parts:
                # Skip R&I and Refinish parts
                part_description = part.get("Description", "").lower()
                if "r&i" in part_description:
                    continue
                
                optimized_part = {
                    "Id": part.get("Id"),
                    "Description": part.get("Description"),
                    "PartDetails": []
                }
                
                part_details = part.get("PartDetails", [])
                for detail in part_details:
                    part_obj = detail.get("Part", {})
                    price_obj = part_obj.get("Price", {})
                    current_price = price_obj.get("CurrentPrice", 0)
                    
                    # Only include expensive parts (>$100)
                    # if current_price > 100:
                    optimized_detail = {
                        "Id": detail.get("Id"),
                        "FullDescription": detail.get("FullDescription"),
                        "Part": {
                            "Description": part_obj.get("Description"),
                            "Price": {"CurrentPrice": current_price}
                        },
                        "AvailableOperations":[]
                    }
                    for operation in detail.get("LaborOperations",[]):
                        print(operation.get("LaborOperationId",""),"LaborOperationId")
                        if AVAILABLE_OPERATIONS.get(operation.get("LaborOperationId","")):
                            optimized_detail["AvailableOperations"].append(AVAILABLE_OPERATIONS.get(operation.get("LaborOperationId")))
                    optimized_part["PartDetails"].append(optimized_detail)
                
                if optimized_part["PartDetails"]:
                    optimized_subcategory["Parts"].append(optimized_part)
            
            if optimized_subcategory["Parts"]:
                optimized_category["SubCategories"].append(optimized_subcategory)
        
        if optimized_category["SubCategories"]:
            optimized_pss["Categories"].append(optimized_category)
    
    return optimized_pss

def extract_car_bio_data(data):
    result = data.get('result',{})
    
    # List of keys to extract
    keys_to_extract = [
        'source',
        'year',
        'make',
        'model',
        'modelID',
        'wmiCountry',
        'wmiManufacturer',
        'buildSource'
    ]

    # Extract the desired key-value pairs
    extracted_data = {}
    missing_keys = []
    for key in keys_to_extract:
        if key in result:
            extracted_data[key] = result[key]
        else:
            missing_keys.append(key)
            extracted_data[key] = None  # or set a default value, e.g., "N/A"

    if missing_keys:
        print(f"Warning: The following keys were not found in 'result' and set to None: {missing_keys}")
    return extracted_data


def extract_images(images_data):
    """
    Extract only required fields from Images data
    """
    if not images_data:
        return []
    
    optimized_images = []
    
    if isinstance(images_data, list):
        for image_obj in images_data:
            optimized_image = {
                "Location": image_obj.get("Location"),
                "Callouts": []
            }
            
            callouts = image_obj.get("Callouts", [])
            for callout in callouts:
                optimized_callout = {
                    "CalloutNumber": callout.get("CalloutNumber"),
                    "PartId": callout.get("PartId")
                }
                optimized_image["Callouts"].append(optimized_callout)
            
            optimized_images.append(optimized_image)
    
    elif isinstance(images_data, dict):
        optimized_image = {
            "Location": images_data.get("Location"),
            "Callouts": []
        }
        
        callouts = images_data.get("Callouts", [])
        for callout in callouts:
            optimized_callout = {
                "CalloutNumber": callout.get("CalloutNumber"),
                "PartId": callout.get("PartId")
            }
            optimized_image["Callouts"].append(optimized_callout)
        
        optimized_images.append(optimized_image)
    
    return optimized_images 