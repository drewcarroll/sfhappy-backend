import re
import json
import requests
from tavily import TavilyClient
from urllib.parse import urljoin
from TavilyURLCrawl.Tavily import TavilyWebCrawl
from ChatGPTContext.ChatInterpret import extract_happy_hour_info
from ExtractTextfromImg.ExtractTextfromImg import extract_text_from_pdfimg
from ExtractSecondaryLinks.ExtractMoreFiles import extract_files_from_results
from ExtractTextNonImgUrl.ExtractTextNonImg import extract_text_nonimg

restaurant_urls = [
    "https://www.bondeliresf.com/",
]

VALID_EXTS = (".pdf", ".png", ".jpg", ".jpeg")

def main():
    all_results = {}
    for base_url in restaurant_urls:
        try:
            response = TavilyWebCrawl(base_url)
            found_files = []
            context = ""
            
            urls = list({item["url"] for item in response["results"]})
            
            # ✅ Extract text from the base URL and all found URLs
            found_files.extend([base_url])  # include base URL
            found_files.extend(urls)  # include found URLs

            for file in found_files:
                text = extract_text_nonimg(file)
                context += file + "\n"
                context += text

            # ✅ Extract more jpg/pdf files from the page content
            file_links = extract_files_from_results(response["results"], base_url, VALID_EXTS)
            found_files.extend(file_links)
            
            # ✅ OCR/parse text from those files
            for file in file_links:
                if file.lower().endswith(VALID_EXTS):
                    context += extract_text_from_pdfimg(file)

            all_results[base_url] = {
                "files": file_links,
                "context": context
            }
            
        except Exception as e:
            print("Error processing {base_url}: {e}")
            all_results[base_url] = {"error": str(e)}

    # ✅ Call ChatGPT to interpret the collected context
    for base_url, result in all_results.items():
        if "error" in result:
            print(f"Error for {base_url}: {result['error']}")
        else:
            print("this is context", result['context'])
            interpretation = extract_happy_hour_info(result["context"])
            with open("happy_hour.json", "w", encoding="utf-8") as f:
                json.dump(interpretation, f, indent=2, ensure_ascii=False)
    
if __name__ == "__main__":
    main()

# Print results