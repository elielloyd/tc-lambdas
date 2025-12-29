import json
import os
from math import ceil

import requests
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
def update_openai_results_with_original_filename(ai_responses, images):
    total_images = len(images)
    print(total_images, len(ai_responses), 'ai+responses + total images')
    for index, ai_response in enumerate(ai_responses):
        if index< total_images:
            ai_response['filename'] = images[index].get('filename',f'image_{index}')
    return ai_responses



# --- Ask GPT to find POIs for a single batch ---
def get_pois_for_batch(
        prompt_original,
        image_filenames,
        image_inputs,
        batch_size=5
):
    start_time = time.time()
    if len(image_inputs)>50:
        batch_size = 10
    # Get API key
    api_key = os.getenv("OPENAI_API_KEY") or ""  # <-- replace with env var only in prod

    headers = {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json"
    }

    total_images = len(image_inputs)
    total_batches = ceil(total_images / batch_size)
    all_results = []

    print(f"Total images: {total_images}, Processing in {total_batches} batch(es)...")

    def process_single_batch(batch_index):
        start = batch_index * batch_size
        end = min(start + batch_size, total_images)

        batch_filenames_local = image_filenames[start:end]
        batch_inputs_local = image_inputs[start:end]

        print(f"🟦 Processing batch {batch_index + 1}/{total_batches} ({len(batch_inputs_local)} images)")

        image_info_local = []
        for i, item in enumerate(batch_filenames_local):
            image_info_local.append({
                "filename": item.get('filename', f'image_{start + i}'),
                "onnx_prediction": item.get('labels', []),
                "reasons": item.get('reasons', '')
            })

        prompt_local = prompt_original.replace("images_placeholder", json.dumps(image_info_local, indent=2))
        prompt_local = prompt_local.replace("input_images_length_placeholder", f'{len(image_info_local)}')

        content_local = [{"type": "input_text", "text": prompt_local}] + batch_inputs_local
        messages_local = [{"role": "user", "content": content_local}]
        payload_local = {
            "model": "gpt-5",
            "input": messages_local,
            "text": {"format": {"type": "json_object"}}
        }

        try:
            response = requests.post(
                "https://api.openai.com/v1/responses",
                headers=headers,
                json=payload_local
            )

            if response.status_code != 200:
                print(f"Batch {batch_index + 1} failed: {response.status_code} - {response.text}")
                return []
            response_data = response.json()
            output_list = response_data.get("output", [])
            assistant_output = next((item for item in output_list if item.get("role") == "assistant"), None)
            if not assistant_output:
                return []
            content = assistant_output.get("content", [])
            if not content:
                print(f"Empty content for batch {batch_index + 1}")
                return []

            raw_text = content[0].get("text", "")
            print(f"Batch {batch_index + 1} response received")
            try:
                parsed = json.loads(raw_text)
                # Check for validation_results (for validation) or damage_results (for damage detection)
                batch_result = parsed.get("validation_results") or parsed.get("damage_results") or parsed
                batch_result = update_openai_results_with_original_filename(batch_result, batch_filenames_local)
                return batch_result
            except Exception as ee:
                print(f"JSON parse error for batch {batch_index + 1}: {ee}")
                print(raw_text)
                return []

        except Exception as e:
            print(f"Exception during batch {batch_index + 1}: {e}")
            return []

    max_workers = min(int(os.getenv("OPENAI_PARALLEL_WORKERS", "10")), total_batches) if total_batches > 0 else 0
    if max_workers == 0:
        print("No batches to process.")
        total_seconds = time.time() - start_time
        print(f"get_pois_for_batch total time: {total_seconds:.2f}s")
        return []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_single_batch, batch_index): batch_index for batch_index in range(total_batches)}
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    all_results.extend(result)
            except Exception as e:
                print(f"Unhandled exception in batch future: {e}")

    print(f"All {total_batches} batches processed successfully.")
    total_seconds = time.time() - start_time
    print(f"get_pois_for_batch total time: {total_seconds:.2f}s")
    return all_results

