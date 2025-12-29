import json
from utils import validate_model

def lambda_handler(event, context):
    try:
        ai_lines= event.get('ai_lines')
        result = validate_model(ai_lines)
        return {
            'success': True,
            'result': result        
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e)
        }   
