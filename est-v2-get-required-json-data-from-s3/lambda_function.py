import json
import boto3
import os
from typing import Dict, List, Any
from utils import get_damage_description_v2, get_s3_file, extract_required_pss_data, extract_car_bio_data

def lambda_handler(event, context):
    """
    Optimized PSS data loader that extracts only required fields
    """
    folder_name = event.get('folder_name')
    
    if not folder_name:
        return {'statusCode': 400, 'body': json.dumps({'error': 'folder_name required'})}

    # Load full PSS data
    pss_key = f'claims/{folder_name}/pre/pss.json'
    full_pss_data = json.loads(get_s3_file(pss_key,{}))
    
    # Extract only required fields
    optimized_pss = extract_required_pss_data(full_pss_data)
    
    # Load other data
    jdpower_key = f'claims/{folder_name}/pre/jdpower.json'
    try:
        jdpower_data =  json.loads(get_s3_file(jdpower_key,{}))
    except Exception as e:
        jdpower_data = {}
    
    coa_answers_key = f'claims/{folder_name}/pre/descriptions/coa_answers.json'
    try:
        coa_answers =  json.loads(get_s3_file(coa_answers_key,{}))
    except Exception as e:
        coa_answers = {}
    
    ea_answers_key = f'claims/{folder_name}/pre/descriptions/ea_answers.json'
    try:
        ea_answers =  json.loads(get_s3_file(ea_answers_key,{}))
    except Exception as e:
        ea_answers = {}

    mitchell_key = f'claims/{folder_name}/est/mitchell.json'
    try:
        mitchell_data = json.loads(get_s3_file(mitchell_key,{}))
    except Exception as e:
        mitchell_key = f'claims/{folder_name}/pre/mitchell.json'
        mitchell_data = json.loads(get_s3_file(mitchell_key,{}))
    car_bio = extract_car_bio_data(jdpower_data)

    categories = optimized_pss.get("Categories", [])
    supercategories = optimized_pss.pop("SuperCategories", [])
    categories_list = []

    for cat in categories:
        # find first matching supercategory
        supercategory_names = []
        for sc in supercategories:
            if cat['Id'] in sc.get('CategoryIds', []):
                supercategory_names.append(sc['Description'])
        supercategory_name = ", ".join(supercategory_names) if supercategory_names else None
        # base info
        cat_info = f"- Id: {cat['Id']}, Name: {cat['Description']}"

        # add supercategory info if found
        if supercategory_name:
            cat_info += f"\n    --SuperCategory: {supercategory_name}"

        # add subcategories
        subcategories = cat.get('SubCategories', [])
        if subcategories:
            cat_info += "\n    --Subcategories:"
            for subcat in subcategories:
                cat_info += f"\n        ---Id: {subcat['Id']}, Description: {subcat['Description']}"

        categories_list.append(cat_info)

    categories_text = "\n".join(categories_list)


    category_list = [cat.get("Description", "Unknown Category") for cat in categories if cat.get("Description")]
    comma_separated_descriptions = ", ".join(category_list)
    damage_description = get_damage_description_v2(folder_name,mitchell_data)
    return {
        'success': True,
        'pss_data': optimized_pss,
        'car_bio': car_bio,
        'mitchell_data': mitchell_data,
        'damage_description':damage_description,
        'category_list': comma_separated_descriptions,
        'categories_text': categories_text,
        'coa_answers':coa_answers,
        'ea_answers':ea_answers
        
    }