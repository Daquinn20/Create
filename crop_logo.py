"""
Crop white space from company logo
"""
from PIL import Image
import numpy as np

# Load image
img = Image.open('company_logo.png')
img_array = np.array(img)

# Convert to grayscale for easier processing
if len(img_array.shape) == 3:
    # If RGBA, use alpha channel if available
    if img_array.shape[2] == 4:
        # Use alpha channel
        gray = img_array[:, :, 3]
    else:
        # Convert to grayscale
        gray = np.mean(img_array[:, :, :3], axis=2)
else:
    gray = img_array

# Find non-white pixels (allowing for small variations)
threshold = 240  # More aggressive - consider anything below 240 as non-white
non_white_pixels = gray < threshold

# Find bounding box
rows = np.any(non_white_pixels, axis=1)
cols = np.any(non_white_pixels, axis=0)

if rows.any() and cols.any():
    top, bottom = np.where(rows)[0][[0, -1]]
    left, right = np.where(cols)[0][[0, -1]]

    # Add small padding
    padding = 20
    top = max(0, top - padding)
    bottom = min(img_array.shape[0], bottom + padding)
    left = max(0, left - padding)
    right = min(img_array.shape[1], right + padding)

    # Crop image
    cropped = img.crop((left, top, right, bottom))

    # Save
    cropped.save('company_logo_cropped.png')
    print(f"Logo cropped successfully!")
    print(f"Original size: {img.size}")
    print(f"Cropped size: {cropped.size}")
else:
    print("Could not find logo content in image")
