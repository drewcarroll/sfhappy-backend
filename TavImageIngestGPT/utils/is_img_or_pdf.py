def is_img_or_pdf(url: str) -> bool:
    IMG_EXTS = (".pdf", ".png", ".jpg", ".jpeg")
    return url.lower().endswith(IMG_EXTS)