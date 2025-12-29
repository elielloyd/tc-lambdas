import onnxruntime as rt
import os
import json
from PIL import Image
import numpy as np
from pathlib import Path
import boto3
import time
import base64
from openai_executions import get_pois_for_batch
from concurrent.futures import ThreadPoolExecutor

# S3 configuration
s3 = boto3.client('s3')
BUCKET_NAME = os.getenv("BUCKET_NAME") or "trueclaim"

def download_file_from_s3(key, local_path):
    """Download a file from S3"""
    s3.download_file(BUCKET_NAME, key, local_path)

def list_s3_files(prefix):
    """List files in S3 with given prefix"""
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
    keys = []
    if "Contents" in response:
        for obj in response["Contents"]:
            keys.append(obj["Key"])
    return keys

def get_and_download_input_images(claim_id):
    """Download images from S3 for a given claim_id"""
    base_est_prefix = f"claims/{claim_id}/est/"
    keys = list_s3_files(base_est_prefix + "InputImages/")
    local_orig_images = []
    print(f"Found {len(keys)} images in S3")
    
    # Create temp directory for downloaded images
    temp_dir = "/tmp/vehicle_images"
    os.makedirs(temp_dir, exist_ok=True)
    
    for key in keys:
        local_file = os.path.join(temp_dir, os.path.basename(key))
        download_file_from_s3(key, local_file)
        local_orig_images.append(local_file)
    
    return local_orig_images


def get_and_download_input_images_parallel(claim_id):
    base_est_prefix = f"claims/{claim_id}/est/"
    keys = list_s3_files(base_est_prefix + "InputImages/")
    temp_dir = "/tmp/vehicle_images"
    os.makedirs(temp_dir, exist_ok=True)

    def download(key):
        local_file = os.path.join(temp_dir, os.path.basename(key))
        download_file_from_s3(key, local_file)
        return local_file

    with ThreadPoolExecutor(max_workers=10) as executor:
        local_orig_images = list(executor.map(download, keys))
    return local_orig_images



position_labels = ['position-Front',
 'position-Front_Left',
 'position-Front_Right',
 'position-Left',
 'position-Rear',
 'position-Rear_Left',
 'position-Rear_Right',
 'position-Right']

label_mapping = {
    'position-Front': ['Front'],
    'position-Front_Left': ['Front','Left'],
    'position-Front_Right': ['Front','Right'],
    'position-Left': ['Left'],
    'position-Rear': ['Rear'],
    'position-Rear_Left': ['Rear','Left'],
    'position-Rear_Right': ['Rear','Right'],
    'position-Right': ['Right']
}

POI_MAPPING = {
    'Front': ['Front'],
    'Right': ['R-Side'],
    'Left': ['L-Side'],
    'Rear': ['Rear'],
    'Roof': ['Roof'],
    'FrontRight': ['Front','R-Side'],
    'FrontLeft': ['Front','L-Side'],
    'RearRight': ['Rear','R-Side'],
    'RearLeft': ['Rear','L-Side'],   
}

def models_loading():
    """Load ONNX model from S3 or local cache with timing"""
    model_file = "ONNX_f1_0.83-positons-res34.onnx"
    local_model_path = f"/tmp/{model_file}"
    if not os.path.exists(local_model_path):
        try:
            # Download model from S3
            s3_key = f"models/{model_file}"
            download_file_from_s3(s3_key, local_model_path)
        except Exception as e:
            print(f"❌ Error downloading model from S3: {e}")
            # Fallback to local file if it exists
            if os.path.exists(model_file):
                local_model_path = model_file
            else:
                raise Exception(f"Model file not found locally or in S3: {model_file}")
    new_position_model = rt.InferenceSession(local_model_path)

    return new_position_model


def image_transform_onnx(path: str, size: int) -> np.ndarray:
    '''Image transform helper for onnx runtime inference.'''
    image = Image.open(path)
    image = image.resize((size,size))
    image = np.array(image)
    image = image.transpose(2,0,1).astype(np.float32)
    image /= 255
    image = image[None, ...]
    return image

def onnx_position_pred(pth,labels = position_labels ,position_model = None, size = 448):

    input_name = position_model.get_inputs()[0].name
    output_name = position_model.get_outputs()[0].name
    processed_input = image_transform_onnx(str(pth),size)
    results = position_model.run([output_name], {input_name: processed_input})[0]
    #labels[np.argmax(results)], results, labels
    return labels[np.argmax(results)]

def map_position_to_labels(position_pred):
    """Map position prediction to standard labels"""
    return label_mapping.get(position_pred, ['Front'])

def classify_vehicle_images_onnx(image_paths):
    """
    Classify multiple vehicle images using ONNX position model
    
    Args:
        image_paths: list of image file paths
        
    Returns:
        dict: classification results in the expected format
    """
    items = []
    new_position_model = models_loading()
    for i, image_path in enumerate(image_paths):
        try:
            position_pred = onnx_position_pred(image_path,position_model=new_position_model, size=448)
            mapped_labels = map_position_to_labels(position_pred)
            item = {
                'filename': os.path.basename(image_path),
                'image_path': image_path,
                'labels': mapped_labels,
                'uncertain': False,  # ONNX model is generally confident
                'reasons': f"ONNX position model prediction: {position_pred} -> {', '.join(mapped_labels)}"
            }
            items.append(item)
            
        except Exception as e:
            print(f"Error processing image {image_path}: {e}")
            item = {
                'filename': os.path.basename(image_path),
                'labels': [],
                'uncertain': True,
                'reasons': f"Error: {str(e)}"
            }
            items.append(item)
    
    return {
        'items': items,
    }

def encode_image_for_openai(image_path):
    """Encode image for OpenAI API with base64"""
    try:
        with open(image_path, "rb") as f:
            base64_img = base64.b64encode(f.read()).decode("utf-8")
            filename = os.path.basename(image_path)

            image_input = {
                "type": "input_image",
                "image_url": "data:image/jpeg;base64," + base64_img,
                
            }
            return filename, image_input
    except Exception as e:
        print(f"Error encoding image {image_path}: {e}")
        return os.path.basename(image_path), None


def create_validation_prompt(custom_prompt=None):
    # Use custom prompt if provided, otherwise use default
    if custom_prompt:
        validation_prompt = custom_prompt
    else:
        validation_prompt = f"""
        You are an expert automotive imaging analyst validating ONNX model predictions for vehicle position classification and performing detailed damage detection.

        # ONNX Model Prediction
        images_placeholder

        # VERY STRICT RULES
        1. Your output must include ALL images provided.
        2. Order of images must ALWAYS remain the same.
        3. ONNX model can only predict: Front, Rear, Left, Right, FrontLeft, FrontRight, RearLeft, RearRight.
        4. You (the AI) CAN additionally classify: Engine / Electrical, Interior, Steering / Suspension, A/C, Frame / Floor.
        5. Only add additional positions to "validated_labels" if you are VERY SURE based on the image.

        # TASK 1 — VALIDATION OF ONNX PREDICTION
        For each image:
        - Review ONNX prediction vs the actual visible region.
        - Confirm or correct the prediction.
        - Provide reasoning.
        - Provide confidence level ("high", "medium", "low").
        - Provide numeric confidence (0–1).
        - Keep "validated_labels" strictly aligned with visible POI.

        # TASK 2 — DAMAGE DETECTION & POI IDENTIFICATION
        For each image, perform full damage analysis:

        1. Detect visible damage (dents, scratches, cracks, broken/missing parts, deformation, misalignment, broken glass, etc.)
        2. Identify ALL damaged regions and map each to a valid POI.
        3. Classify each damage region as:
        - PRIMARY — main point of impact
        - SECONDARY — minor or related damage
        4. Assess severity:
        - MAJOR — structural or heavy damage
        - MINOR — light/cosmetic damage
        5. Provide a short descriptive explanation for each damaged region.
        6. Add "has_damage": true to the output if there is any damage in the image.

        # VALID POI OPTIONS
        "Right Front Corner", "Right Front Side", "Right Side", "Right Rear Side",
        "Right Rear Corner", "Rear", "Left Rear Corner", "Left Rear Side",
        "Left Side", "Left Front Side", "Left Front Corner", "Front", "Roof",
        "Engine / Electrical", "Interior", "Steering / Suspension", "A/C", "Frame / Floor"

        # POI MAPPING DEFINITIONS
        {{
        "Right Front Corner": ["Right Headlight", "Right Front Bumper Corner", "Right Fender (Front Portion)", "Right Fog Light (if equipped)"],
        "Right Front Side": ["Right Front Fender", "Right Side Mirror", "Right A-Pillar"],
        "Right Side": ["Right Front Door", "Right Rear Door", "Right Side Skirts", "Right B-Pillar"],
        "Right Rear Side": ["Right Quarter Panel", "Right Rear Wheel Arch"],
        "Right Rear Corner": ["Right Tail Light", "Rear Bumper (Right Corner)", "Right Rear Fender Extension"],
        "Rear": ["Trunk / Boot Lid", "Rear Windshield", "Rear Bumper", "Number Plate Area"],
        "Left Rear Corner": ["Left Tail Light", "Rear Bumper (Left Corner)", "Left Rear Fender Extension"],
        "Left Rear Side": ["Left Quarter Panel", "Left Rear Wheel Arch"],
        "Left Side": ["Left Front Door", "Left Rear Door", "Left Side Skirts", "Left B-Pillar"],
        "Left Front Side": ["Left Front Fender", "Left Side Mirror", "Left A-Pillar"],
        "Left Front Corner": ["Left Headlight", "Left Front Bumper Corner", "Left Fender (Front Portion)", "Left Fog Light (if equipped)"],
        "Front": ["Front Bumper", "Front Grill", "Bonnet / Hood", "Front Windshield"],
        "Roof": ["Roof Panel", "Roof Rails (if equipped)", "Sunroof / Moonroof (if equipped)"],
        "Engine / Electrical": ["Engine components", "Electrical systems", "Battery", "Wiring"],
        "Interior": ["Interior components", "Seats", "Dashboard", "Interior panels"],
        "Steering / Suspension": ["Steering components", "Suspension parts", "Wheels", "Axles"],
        "A/C": ["Air conditioning system", "HVAC components"],
        "Frame / Floor": ["Vehicle frame", "Floor panels", "Structural components"]
        }}

        # TASK 3 — OUTPUT FORMAT
        The "validation_results" array length must equal: input_images_length_placeholder

        # STRICT JSON OUTPUT FORMAT
        Return ONLY JSON in the following structure:
        {{
        "validation_results": [
            {{
            "filename": "<string>",
            "onnx_prediction": ["Front","Left","Right","Rear","FrontLeft","FrontRight","RearLeft","RearRight"],
            "validated_labels": ["Front","Left","Right","Rear","Engine / Electrical"],
            "is_correct": true,
            "confidence": "high",
            "confidence_number": 0.98,
            "reasoning": "<explanation>",
            "changes_made": "<what changed, if anything>",
            "has_damage": true | false,
            "damage_regions": [
                {{
                "poi": "Front",
                "severity": "major",
                "type": "primary",
                "description": "Front bumper heavily deformed with broken grille"
                }},
                {{
                "poi": "Left Front Corner",
                "severity": "minor",
                "type": "secondary",
                "description": "Light scratches near left front fender"
                }}
            ]
            }}
        ]
        }}

        Return only the JSON, nothing else.
    """
    return validation_prompt

def validate_onnx_with_openai(onnx_results, custom_prompt=None):
    # Create validation prompt
    validation_prompt = create_validation_prompt(custom_prompt)

    image_filenames = []
    image_inputs = []
    valid_items = []
    for item in onnx_results['items']:
        if item.get('image_path'):
            filename, image_input = encode_image_for_openai(item['image_path'])
            if image_input:  # Only add if encoding was successful
                image_filenames.append(item)
                image_inputs.append(image_input)
                valid_items.append(item)
    
    if not image_inputs:
        print("No valid images found for OpenAI validation")
        return []
    
    try:
        print(f"Making single OpenAI API call for {len(image_inputs)} images")
        openai_validation = get_pois_for_batch(
            validation_prompt, 
            image_filenames,
            image_inputs
        )
        # Handle both single result and list of results
        if isinstance(openai_validation, list):
            return openai_validation
        elif isinstance(openai_validation, dict) and 'error' not in openai_validation:
            return [openai_validation]
        else:
            return []
            
    except Exception as e:
        print(f"Error during OpenAI validation: {e}")
        return []
    

def combine_onnx_openai_results(onnx_results, openai_validation):
    """Combine ONNX and OpenAI results with damage detection"""
    combined_results = {
        # 'onnx_predictions': onnx_results,
        'openai_validation': openai_validation,
        'final_results': [],
        'damage_pois': [],
        'all_damaged_regions': []  # All damaged regions for reference
    }
    
    # Check if OpenAI validation was successful
    if openai_validation and isinstance(openai_validation, list):
        updated_items = []
        for item in onnx_results.get('items', []):
            filename = item['filename']
            
            # Find corresponding validation result from the list
            validation_item = next(
                (v for v in openai_validation 
                 if v.get('filename') == filename), 
                None
            )
            if validation_item:
                # Use OpenAI result if confidence is high, otherwise use ONNX
                if validation_item.get('confidence_number') >= 0.6:
                    item['labels'] = validation_item.get('validated_labels', item['labels'])
                    item['reasons'] = f"ONNX: {item.get('reasons', '')} | OpenAI Validation: {validation_item.get('reasoning', '')}"
                    item['source'] = 'OpenAI (high confidence)'
                else:
                    item['reasons'] = f"ONNX: {item.get('reasons', '')} | OpenAI Low Confidence: {validation_item.get('reasoning', '')}"
                    item['source'] = 'ONNX (OpenAI low confidence)'
                item['is_onnx_correct'] = validation_item.get('is_correct', False)
                item['openai_confidence'] = validation_item.get('confidence', 'medium')
            updated_items.append(item)
        
        # Convert to POI format
        combined_results['final_results'] = convert_to_poi_format({'items': updated_items})
    else:
        combined_results['final_results'] = convert_to_poi_format(onnx_results)
    
    # Process damage detection results
    if openai_validation and isinstance(openai_validation, list):
        # Extract all damaged regions for reference
        all_regions = []
        for damage_item in openai_validation:
            filename = damage_item.get("filename")
            has_damage = damage_item.get("has_damage", False)
            damage_regions = damage_item.get("damage_regions", [])
            
            if has_damage and damage_regions:
                for region in damage_regions:
                    all_regions.append({
                        "filename": filename,
                        "poi": region.get("poi", "").strip(),
                        "severity": region.get("severity", "major"),
                        "type": region.get("type", "primary"),
                        "description": region.get("description", "")
                    })
        
        combined_results['all_damaged_regions'] = all_regions
        
        # Only include primary POI (point of impact) in damage_pois
        combined_results['damage_pois'] = convert_to_damage_poi_format(openai_validation)
    else:
        combined_results['damage_pois'] = []
        combined_results['all_damaged_regions'] = []
    
    return combined_results


def map_poi_to_standard_format(poi):
    """
    Map POI values to standard format and split combined POIs.
    
    Args:
        poi: POI string (e.g., "Front", "FrontLeft", "Rear", etc.)
        
    Returns:
        list: List of standard POI strings
    """
    poi = poi.strip()
    
    # Available standard POI options
    valid_pois = {
        "Front", "Rear", "Left", "Right",
        "Engine / Electrical", "Interior", "Steering / Suspension", "A/C", "Frame / Floor"
    }
    
    # Mapping for combined POIs
    combined_poi_mapping = {
        "FrontLeft": ["Front", "Left"],
        "FrontRight": ["Front", "Right"],
        "RearLeft": ["Rear", "Left"],
        "RearRight": ["Rear", "Right"]
    }
    
    # Check if it's a combined POI
    if poi in combined_poi_mapping:
        return combined_poi_mapping[poi]
    
    # If it's already a valid POI, return it as a list
    if poi in valid_pois:
        return [poi]
    
    # For "Roof" or other unmapped values, return empty list (or map as needed)
    # You can add custom mapping here if needed
    if poi == "Roof":
        return []  # Roof is not in the standard list, exclude it
    
    # If it's not recognized, try to return as-is if it matches a valid POI (case-insensitive)
    poi_lower = poi.lower()
    for valid_poi in valid_pois:
        if valid_poi.lower() == poi_lower:
            return [valid_poi]
    
    # Return empty list for unrecognized POIs
    return []

def convert_to_damage_poi_format(damage_results):
    """
    Convert damage detection results to a simple list of unique POI strings.
    Filters to only include primary POIs and maps them to standard format.
    
    Args:
        damage_results: List of damage detection results
        
    Returns:
        list: List of unique damage POI strings, e.g., ["Rear", "Left"]
    """
    if not isinstance(damage_results, list):
        return []
    
    # First pass: collect all damage regions with their metadata
    all_damage_regions = []
    for damage_item in damage_results:
        filename = damage_item.get("filename")
        has_damage = damage_item.get("has_damage", False)
        damage_regions = damage_item.get("damage_regions", [])
        
        if has_damage and damage_regions:
            for region in damage_regions:
                poi = region.get("poi", "").strip()
                if poi:
                    all_damage_regions.append({
                        "filename": filename,
                        "poi": poi,
                        "severity": region.get("severity", "major"),
                        "type": region.get("type", "primary"),
                        "description": region.get("description", "")
                    })
    
    if not all_damage_regions:
        return []
    
    # Determine if there's any major primary damage
    major_primary_damage = any(
        r.get("severity") == "major" and r.get("type") == "primary"
        for r in all_damage_regions
    )
    filtered_regions = []
    for region in all_damage_regions:
        region_type = region.get("type", "primary")
        region_severity = region.get("severity", "major")
        
        if major_primary_damage:
            # Only include primary regions when major primary damage exists
            if region_type == "primary":
                filtered_regions.append(region)
        else:
            # Include all primary regions if no major primary damage
            if region_type == "primary":
                filtered_regions.append(region)
    
    # Collect all unique POIs after mapping
    unique_pois = set()
    for region in filtered_regions:
        poi_name = region["poi"]
        # mapped_pois = map_poi_to_standard_format(poi_name)
        unique_pois.update([poi_name])
    
    # Convert to sorted list for consistent output
    return sorted(list(unique_pois))

def convert_to_poi_format(results):
    """
    Convert results to POI format where each position has its own object with images array
    
    Args:
        results: Results dictionary with 'items' containing classification results
        
    Returns:
        list: List of POI objects with format:
        [
            {
                "poi": "Front",
                "images": ["image1.jpeg", "image2.jpeg"]
            },
            {
                "poi": "Rear", 
                "images": ["image3.jpeg"]
            }
        ]
    """
    poi_dict = {}
    
    for item in results.get('items', []):
        filename = item['filename']
        labels = item.get('labels', [])
        
        # Handle multiple labels for a single image
        for label in labels:
            updated_labels = POI_MAPPING.get(label, [label])
            for updated_label in updated_labels:
                updated_label = updated_label.strip()
                if updated_label not in poi_dict:
                    poi_dict[updated_label] = []
                poi_dict[updated_label].append(filename)
    
    # Convert to the required format
    final_results = []
    for poi, images in poi_dict.items():
        final_results.append({
            "poi": poi,
            "images": list(set(images))
        })
    
    return final_results

def classify_vehicle_images_onnx_with_openai(image_paths, validate_with_openai=True, detect_damage=True, custom_prompt=None, damage_detection_prompt=None):
    """
    Classify multiple vehicle images using ONNX model and optionally validate with OpenAI and detect damage
    
    Args:
        image_paths: list of image file paths
        validate_with_openai: whether to validate results with OpenAI
        detect_damage: whether to detect damage POIs from images
        custom_prompt: custom prompt template for OpenAI validation
        damage_detection_prompt: custom prompt template for damage detection
        
    Returns:
        dict: classification results with validation and damage detection
    """
    onnx_results = classify_vehicle_images_onnx(image_paths)
    if not validate_with_openai and not detect_damage:
        return {
            'results': onnx_results,
        }
    
    openai_validation = []
    
    # Perform OpenAI validation if requested
    if validate_with_openai:
        openai_validation = validate_onnx_with_openai(onnx_results, custom_prompt)
    
    combined_results = combine_onnx_openai_results(onnx_results, openai_validation)
    
    return combined_results


def lambda_handler(event, context):
    """
    AWS Lambda handler function
    
    Args:
        event: Lambda event containing claim_id, validate_with_openai, detect_damage, and optionally custom_prompt, damage_detection_prompt
        context: Lambda context
        
    Returns:
        dict: Lambda response with classification results and damage POIs
    """
    claim_id = event.get('claim_id')
    validate_with_openai = event.get('validate_with_openai', True)
    detect_damage = event.get('detect_damage', True)  # Enable damage detection by default
    custom_prompt = event.get('custom_prompt', None)
    damage_detection_prompt = event.get('damage_detection_prompt', None)
    
    image_paths = get_and_download_input_images_parallel(claim_id)
    results = classify_vehicle_images_onnx_with_openai(
        image_paths, 
        validate_with_openai=validate_with_openai,
        detect_damage=detect_damage,
        custom_prompt=custom_prompt,
        damage_detection_prompt=damage_detection_prompt
    )
    return results