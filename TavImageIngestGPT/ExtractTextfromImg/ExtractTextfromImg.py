import os
import io
import requests
import pdfplumber
from pdf2image import convert_from_bytes
import pytesseract
from PIL import Image

def extract_text_from_pdfimg(file_url: str) -> str:
    """
    Extract text from a remote file (PDF, PNG, JPG).
    - For PDFs: tries pdfplumber first, falls back to OCR per page.
    - For images: uses OCR directly.
    """
    print(f"Fetching {file_url}")
    resp = requests.get(file_url)
    resp.raise_for_status()
    content = resp.content

    ext = os.path.splitext(file_url.split("?")[0])[1].lower()
    all_text = []

    if ext == ".pdf":
        with pdfplumber.open(io.BytesIO(content)) as pdf:
            for page in pdf.pages:
                txt = page.extract_text()
                if txt and txt.strip():
                    all_text.append(txt)
                else:
                    img = convert_from_bytes(
                        content, dpi=300,
                        first_page=page.page_number,
                        last_page=page.page_number
                    )[0]
                    ocr_txt = pytesseract.image_to_string(img)
                    all_text.append(ocr_txt)

    elif ext in (".png", ".jpg", ".jpeg"):
        img = Image.open(io.BytesIO(content))
        ocr_txt = pytesseract.image_to_string(img)
        all_text.append(ocr_txt)
        

    else:
        raise ValueError(f"Unsupported file type: {ext}")

    return "\n".join(all_text)
