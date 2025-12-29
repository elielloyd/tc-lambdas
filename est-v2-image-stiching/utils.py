import boto3
import json
import os
import base64
import math
from PIL import Image, ImageOps, ImageDraw

s3 = boto3.client('s3')
BUCKET_NAME = os.getenv("BUCKET_NAME") or  "trueclaim"
def download_file_from_s3(key, local_path):
    try:
        s3.download_file(BUCKET_NAME, key, local_path)
        return True
    except Exception as e:
        print(f"Error downloading file from S3: {e}")
        return False


def get_and_download_input_images(claim_id, images, isCompleteKey=False):
    base_est_prefix = "" if isCompleteKey else f"claims/{claim_id}/est/"
    local_orig_images= []
    local_base_path = f"/tmp/" if isCompleteKey else f"/tmp/claims/{claim_id}/est/InputImages/"
    os.makedirs(local_base_path, exist_ok=True)
    for image in images:
        s3_key =  image if isCompleteKey else f"claims/{claim_id}/est/InputImages/{image}"
        print(s3_key)
        local_path = os.path.join(local_base_path, image)
        # ensure any subfolder paths exist
        os.makedirs(os.path.dirname(local_path), exist_ok=True)

        image_found = download_file_from_s3(s3_key, local_path)
        if image_found:
            local_orig_images.append(local_path)
    return local_orig_images


def compress_image_for_damage_detection(image_path, max_size_kb=500, max_dimension=1200):
    """
    Compresses an image while maintaining quality suitable for damage detection.
    Targets max 500KB file size and max 1200px on longest side.
    """
    try:
        # Open and correct orientation
        img = ImageOps.exif_transpose(Image.open(image_path))
        
        # Calculate new dimensions while maintaining aspect ratio
        width, height = img.size
        if max(width, height) > max_dimension:
            if width > height:
                new_width = max_dimension
                new_height = int((height * max_dimension) / width)
            else:
                new_height = max_dimension
                new_width = int((width * max_dimension) / height)
            img = img.resize((new_width, new_height), Image.Resampling.LANCZOS)
        
        # Convert to RGB if necessary (for JPEG)
        if img.mode in ('RGBA', 'LA', 'P'):
            img = img.convert('RGB')
        
        # Compress to target file size
        max_size_bytes = max_size_kb * 1024
        
        # Start with high quality and reduce if needed
        for quality in range(95, 20, -5):
            # Save to bytes buffer to check size
            import io
            buffer = io.BytesIO()
            img.save(buffer, format='JPEG', quality=quality, optimize=True)
            buffer_size = buffer.tell()
            
            if buffer_size <= max_size_bytes:
                print(f"Image compressed to {buffer_size/1024:.1f}KB with quality {quality}")
                return buffer.getvalue()
        
        # If still too large, reduce dimensions further
        print(f"Image still too large, reducing dimensions further...")
        img = img.resize((int(img.width * 0.8), int(img.height * 0.8)), Image.Resampling.LANCZOS)
        
        # Try again with reduced size
        for quality in range(85, 20, -5):
            buffer = io.BytesIO()
            img.save(buffer, format='JPEG', quality=quality, optimize=True)
            buffer_size = buffer.tell()
            
            if buffer_size <= max_size_bytes:
                print(f"Image compressed to {buffer_size/1024:.1f}KB with quality {quality} and reduced dimensions")
                return buffer.getvalue()
        
        # Last resort: very aggressive compression
        buffer = io.BytesIO()
        img.save(buffer, format='JPEG', quality=30, optimize=True)
        print(f"Image compressed to {buffer.tell()/1024:.1f}KB with aggressive compression")
        return buffer.getvalue()
        
    except Exception as e:
        raise Exception(f"Failed to compress image: {e}")


def encode_image_from_url_to_buffer(image_url, compress=True, max_size_kb=500):
    """
    Encodes an image from a given URL or local file path into a base64 string.
    Optionally compresses the image for damage detection use.
    """
    try:
        # Check if the input is a local file path
        if os.path.isfile(image_url):
            if compress:
                # Use compressed version for damage detection
                image_data = compress_image_for_damage_detection(image_url, max_size_kb)
                print("Image loaded and compressed successfully from local file path.")
            else:
                # Open the local file and read its content
                with open(image_url, "rb") as image_file:
                    image_data = image_file.read()
                print("Image loaded successfully from local file path.")
        else:
            # Handle it as a URL (assumes it's a web URL)
            import requests
            response = requests.get(image_url)
            response.raise_for_status()  # Raise an error for bad status codes
            image_data = response.content
            print("Image loaded successfully from URL.")
            
            # If URL and compression requested, we'd need to save temporarily first
            if compress:
                # Save temporarily, compress, then use compressed data
                temp_path = f"/tmp/temp_image_{hash(image_url)}.jpg"
                with open(temp_path, "wb") as f:
                    f.write(image_data)
                image_data = compress_image_for_damage_detection(temp_path, max_size_kb)
                os.remove(temp_path)  # Clean up temp file

        # Encode the image content in base64
        return base64.b64encode(image_data).decode('utf-8')

    except Exception as e:
        raise Exception(f"Failed to retrieve or encode image: {e}")
    


def stitch_images(image_paths, claim_id, output_size=(1280, 1280)):
    # Open images and correct orientation using EXIF metadata
    images = [
        ImageOps.exif_transpose(Image.open(path))
        for path in image_paths
        if str(path).lower().endswith(('.png', '.jpg', '.jpeg','gif','webp'))
    ]

    # Determine the grid size dynamically based on the number of images
    print(image_paths)
    grid_size = math.ceil(math.sqrt(len(images)))

    # Calculate the size for each individual image
    individual_size = (output_size[0] // grid_size, output_size[1] // grid_size)

    # Create a blank white canvas for the final image
    stitched_image = Image.new('RGB', output_size, (255, 255, 255))
    draw = ImageDraw.Draw(stitched_image)

    for idx, img in enumerate(images):
        # Resize the image to the determined size
        resized_img = img.resize(individual_size)

        # Calculate position for the image in the grid
        x_offset = (idx % grid_size) * individual_size[0]
        y_offset = (idx // grid_size) * individual_size[1]

        # Paste the resized image to the final canvas
        stitched_image.paste(resized_img, (x_offset, y_offset))
        
        # Add dashed border around each image
        border_width = 3
        dash_length = 10
        gap_length = 5
        
        # Calculate border coordinates
        left = x_offset
        top = y_offset
        right = x_offset + individual_size[0] - 1
        bottom = y_offset + individual_size[1] - 1
        
        # Draw dashed border
        _draw_dashed_rectangle(draw, left, top, right, bottom, 
                              border_width, dash_length, gap_length, (0, 0, 0))

    stitched_image_path = os.path.join(f"/tmp/claims/{claim_id}/est", f"stitched.jpeg")
    os.makedirs(os.path.dirname(stitched_image_path), exist_ok=True)
    stitched_image.save(stitched_image_path)
    return stitched_image, stitched_image_path


def _draw_dashed_rectangle(draw, left, top, right, bottom, width, dash_length, gap_length, color):
    """
    Draws a dashed rectangle border around an image.
    """
    # Top border
    _draw_dashed_line(draw, left, top, right, top, width, dash_length, gap_length, color)
    # Bottom border
    _draw_dashed_line(draw, left, bottom, right, bottom, width, dash_length, gap_length, color)
    # Left border
    _draw_dashed_line(draw, left, top, left, bottom, width, dash_length, gap_length, color)
    # Right border
    _draw_dashed_line(draw, right, top, right, bottom, width, dash_length, gap_length, color)


def _draw_dashed_line(draw, x1, y1, x2, y2, width, dash_length, gap_length, color):
    """
    Draws a dashed line between two points.
    """
    # Calculate total length
    if x1 == x2:  # Vertical line
        total_length = abs(y2 - y1)
        is_vertical = True
    else:  # Horizontal line
        total_length = abs(x2 - x1)
        is_vertical = False
    
    # Calculate number of dashes and gaps
    dash_gap_length = dash_length + gap_length
    num_dashes = int(total_length / dash_gap_length)
    
    # Draw dashes
    for i in range(num_dashes):
        start_offset = i * dash_gap_length
        end_offset = start_offset + dash_length
        
        if is_vertical:
            start_y = min(y1, y2) + start_offset
            end_y = min(y1, y2) + end_offset
            draw.rectangle([x1 - width//2, start_y, x1 + width//2, end_y], fill=color)
        else:
            start_x = min(x1, x2) + start_offset
            end_x = min(x1, x2) + end_offset
            draw.rectangle([start_x, y1 - width//2, end_x, y1 + width//2], fill=color)