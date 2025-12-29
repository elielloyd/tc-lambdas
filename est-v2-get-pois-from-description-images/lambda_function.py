import json
from utils import get_and_download_input_images, process_images_with_user_description

def lambda_handler(event, context):
    try:
        claim_id = event.get('claim_id')
        prompt = event.get('prompt')
        images = get_and_download_input_images(claim_id)
        pois =process_images_with_user_description(prompt,images)
        poi_results = []
        for poi, images in pois.items():
            if len(images) > 0: 
                poi_results.append({
                    'poi': poi,
                    'images': images
                })

        return {
            'success':True,
            'claim_id':claim_id,
            'images':images,
            'pois':poi_results
        }
    except Exception as e:
        print(e)
        return {'success': False, 'error': str(e),'event':event}
