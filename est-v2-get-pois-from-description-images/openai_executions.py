import openai
import json
import os
POIS = [
    'Front', 'R-Front-Corner', 'R-Front-Side','R-Side','R-Rear-Side',
    'Rear','L-Rear-Side','L-Side','L-Front-Side','Roof'
]

openai.api_key = os.getenv("OPENAI_API_KEY")

# --- Ask GPT to find POIs for a single batch ---
def get_pois_for_batch(
    prompt_original,
    image_filenames,
    image_inputs
):
    prompt = prompt_original.replace("{image_filenames}", json.dumps(image_filenames))
    print(f"prompt: {prompt}", len(image_inputs))
    messages = [{"role":"system", "content": "You are a car mechanic and you help find out point of impact (POIs) using user description and images."},{"role": "user", "content": [{"type": "text", "text": prompt}] + image_inputs}]
    try:
        response = openai.chat.completions.create(
            model="gpt-4o-2024-11-20",
            messages=messages,
            max_tokens=16384,
            response_format={"type": "json_object"},
            temperature=0.3
        )

        content = response.choices[0].message.content.strip()
        print(f"content pois: {content}")
        pois_dict = eval(content)  # Consider json.loads() for safer parsing
        return {poi: imgs for poi, imgs in pois_dict.items() if poi in POIS}
    except Exception as e:
        print("Batch error:", e)
        return {}
