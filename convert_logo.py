"""
Convert company logo PDF to PNG for dashboard use
"""
try:
    # Try using PyMuPDF (fitz)
    import fitz

    # Open PDF
    pdf_path = "Company logo.pdf"
    output_path = "company_logo.png"

    pdf_document = fitz.open(pdf_path)
    page = pdf_document[0]  # Get first page

    # Convert to image with high resolution
    zoom = 3  # Increase resolution
    mat = fitz.Matrix(zoom, zoom)
    pix = page.get_pixmap(matrix=mat, alpha=True)

    # Save as PNG
    pix.save(output_path)
    pdf_document.close()

    print(f"✓ Logo converted successfully: {output_path}")

except ImportError:
    print("PyMuPDF not found, trying pdf2image...")
    try:
        from pdf2image import convert_from_path
        from PIL import Image

        # Convert PDF to images
        images = convert_from_path("Company logo.pdf", dpi=300)

        # Save first page as PNG
        if images:
            images[0].save("company_logo.png", "PNG")
            print("✓ Logo converted successfully: company_logo.png")
    except ImportError:
        print("Installing required packages...")
        import subprocess
        subprocess.run(["pip", "install", "PyMuPDF"])
        print("Please run this script again.")
