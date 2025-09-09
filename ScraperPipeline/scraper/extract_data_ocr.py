import os
import io
import requests
import pdfplumber
from pdf2image import convert_from_bytes
import pytesseract
from PIL import Image

def extract_data_ocr(file_url: str) -> str:
    all_text = []

    if file_url.startswith("http://") or file_url.startswith("https://"):
        # Fetch remote file
        resp = requests.get(file_url)
        resp.raise_for_status()
        content = resp.content
    else:
        # Local file
        with open(file_url, "rb") as f:
            content = f.read()

    ext = os.path.splitext(file_url.split("?")[0])[1].lower()

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
