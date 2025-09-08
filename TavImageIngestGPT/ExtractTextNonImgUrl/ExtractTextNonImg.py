from bs4 import BeautifulSoup
from urllib.parse import urljoin
import requests
import re

def extract_text_nonimg(file_url: str) -> str:
    if not file_url.startswith("http"):
        file_url = urljoin("http://", file_url)

    response = requests.get(file_url, timeout=15)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    hh_blocks = []

    # regex for time ranges like 3:00 PM — 5:00 PM
    time_pattern = re.compile(r"\d{1,2}:\d{2}\s?(AM|PM).{0,3}\d{1,2}:\d{2}\s?(AM|PM)")

    # Find all nodes mentioning Happy Hour
    for tag in soup.find_all(string=re.compile("happy hour", re.I)):
        parent = tag.find_parent()

        # Collect nearby text (siblings + parent text)
        block_texts = []
        if parent:
            block_texts.append(parent.get_text(" ", strip=True))

            # Look at siblings above and below
            for sib in parent.find_next_siblings(limit=2):
                block_texts.append(sib.get_text(" ", strip=True))
            for sib in parent.find_previous_siblings(limit=2):
                block_texts.append(sib.get_text(" ", strip=True))

        # Merge and pull times
        merged = " ".join(block_texts)
        times = time_pattern.findall(merged)
        hh_blocks.append(merged)
        print(hh_blocks)

    return "\n".join(hh_blocks)