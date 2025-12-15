"""
Convert and crop company logo - keep only top portion
"""
import fitz
from PIL import Image

# Convert PDF to PNG
pdf_document = fitz.open("Company logo.pdf")
page = pdf_document[0]

# Convert to high resolution image
zoom = 3
mat = fitz.Matrix(zoom, zoom)
pix = page.get_pixmap(matrix=mat, alpha=True)
pix.save("company_logo_full.png")
pdf_document.close()

# Load the full image
img = Image.open("company_logo_full.png")
width, height = img.size

# Crop to keep only top 50% (adjust this percentage if needed)
# This should keep the logo and tagline, removing content below
crop_height = int(height * 0.5)  # Keep top 50%

# Crop image
cropped = img.crop((0, 0, width, crop_height))

# Save
cropped.save("company_logo.png")

print(f"Logo converted and cropped!")
print(f"Original: {img.size}")
print(f"Cropped: {cropped.size}")
