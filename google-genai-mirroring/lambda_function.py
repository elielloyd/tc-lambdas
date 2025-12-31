import base64
import json
import os
import boto3
import requests
from google import genai
from google.genai.types import (
    GenerateContentConfig,
    ThinkingConfig,
    Part,
)

PROJECT_ID = os.environ.get("GOOGLE_CLOUD_PROJECT")
LOCATION = "global"
MODEL_ID = "gemini-2.5-flash"

client = genai.Client(vertexai=True, project=PROJECT_ID, location=LOCATION)
thinking_config = ThinkingConfig(thinking_budget=0)

s3 = boto3.client("s3")


def load_pdf_from_s3(s3_url):
    s3_url = s3_url.replace("s3://", "")
    bucket, key = s3_url.split("/", 1)
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read()


def load_pdf_from_url(url):
    r = requests.get(url, timeout=200)
    r.raise_for_status()
    return r.content

default_response_schema = {
  "type": "object",
  "properties": {
    "name": {
      "type": "string"
    },
    "vehicle_name": {
      "type": "string"
    },
    "vin": {
      "type": "string"
    },
    "odometer": {
      "type": "string"
    },
    "insurance_company": {
      "type": "string"
    },
    "lines": {
      "type": "array",
      "items": {
        "type": "object",
        "properties": {
          "header": {
            "type": "string"
          },
          "dbRef": {
            "type": "string"
          },
          "description": {
            "type": "string"
          },
          "operation": {
            "type": "string"
          },
          "Type": {
            "type": "string"
          },
          "TotalUnits": {
            "type": "string"
          },
          "Type2": {
            "type": "string"
          },
          "Number": {
            "type": "string"
          },
          "Qty": {
            "type": "string"
          },
          "TotalPrice": {
            "type": "string"
          },
          "Tax": {
            "type": "string"
          }
        },
        "required": [
          "header",
          "dbRef",
          "description",
          "operation",
          "Type",
          "TotalUnits",
          "Type2",
          "Number",
          "Qty",
          "TotalPrice",
          "Tax"
        ]
      }
    },
    "type": {
      "type": "string"
    }
  },
  "required": [
    "name",
    "vehicle_name",
    "vin",
    "odometer",
    "insurance_company",
    "lines",
    "type"
  ]
}

def lambda_handler(event, context):
    """
    Expected event format:
    {
      "system_prompt": "...",
      "pdf_base64": "JVBERi0xLjc...."   OR
      "pdf_s3_url": "s3://bucket/file.pdf" OR
      "pdf_url": "https://domain.com/file.pdf"
    }
    """
    try:
        system_prompt = event.get("system_prompt", "")
        if not system_prompt:
            raise Exception("required_field is missing")

        # Use Response Schema from payload if available
        active_schema = event.get("response_schema", default_response_schema)

        # Load PDF bytes
        if "pdf_base64" in event:
            pdf_bytes = base64.b64decode(event["pdf_base64"])
        elif "pdf_s3_url" in event:
            pdf_bytes = load_pdf_from_s3(event["pdf_s3_url"])
        elif "pdf_url" in event:
            pdf_bytes = load_pdf_from_url(event["pdf_url"])
        else:
            raise Exception("required_field is missing")
        response = client.models.generate_content(
            model=MODEL_ID,
            contents=[Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")],
            config=GenerateContentConfig(
                system_instruction=system_prompt,
                response_mime_type="application/json",
                response_schema=active_schema,
                # thinking_config=thinking_config,
                temperature=0.01,
            ),
        )

        return json.loads(response.text)
    except Exception as e:
        raise Exception(str(e))
    

