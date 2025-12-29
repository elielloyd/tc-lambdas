import json
import traceback
import sys
from utils import stitch_images, get_and_download_input_images, encode_image_from_url_to_buffer

def lambda_handler(event, context):
    try:
        claim_id = event['claim_id']
        poi = event['poi']
        images = event['images']
        return_all_images = event.get('return_all_images', False)
        isCompleteKey = event.get('isCompleteKey', False)
        

        # Download images locally
        local_images_paths = get_and_download_input_images(claim_id, images, isCompleteKey)

        # Stitch images first
        stiched_image, stitched_image_path = stitch_images(local_images_paths, claim_id)
        encoded_stitched_image = encode_image_from_url_to_buffer(stitched_image_path, compress=True, max_size_kb=500)
        
        # Check if we should include individual images based on payload size
        MAX_PAYLOAD_SIZE = 5900000  # 5.9MB limit
        
        # Always include stitched image
        response = {
            'success': True,
            'poi':poi,
            'stitched_image': encoded_stitched_image,
            'all_images_base64': [],
            'total_images': len(local_images_paths),
            'payload_size_check': 'stitched_only'
        }
        
        # Check if individual images should be included
        if return_all_images and len(local_images_paths) <= 12:
            # Encode individual images
            encoded_images = []
            for img_path in local_images_paths:
                encoded_images.append(encode_image_from_url_to_buffer(img_path, compress=True, max_size_kb=500))
            
            # Create response with individual images
            response_with_individuals = {
                'success': True,
                 'poi':poi,
                'stitched_image': encoded_stitched_image,
                'all_images_base64': encoded_images,
                'total_images': len(encoded_images),
                'payload_size_check': 'with_individuals'
            }
            
            # Check payload size
            response_json = json.dumps(response_with_individuals)
            payload_size = sys.getsizeof(response_json)
            
            print(f"Payload size with individual images: {payload_size} bytes")
            
            if payload_size <= MAX_PAYLOAD_SIZE:
                # Safe to include individual images
                response = response_with_individuals
                response['payload_size_check'] = 'with_individuals'
                response['payload_size'] = payload_size
                print("Including individual images - payload size OK")
            else:
                # Too large, return only stitched image
                response['payload_size_check'] = 'stitched_only_due_to_size'
                print(f"Payload too large ({payload_size} bytes), returning only stitched image")
        else:
            print("Individual images not requested or too many images")
        
        return response
    except Exception as e:
        error_trace = traceback.format_exc()
        print("Error occurred:", error_trace)
        return {
            'success': False,
            'stitched_image':'',
            'all_images_base64': [],
            'total_images': 0,
            'body': json.dumps(str(e))
        }
