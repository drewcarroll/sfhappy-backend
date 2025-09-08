import json
import TavImageIngestGPT.utils.is_img_or_pdf as is_img_or_pdf
import extract_urls_from_tavily
import TavImageIngestGPT.scraper.tavily_web_crawl as tavily_web_crawl
import extract_data_ocr
import create_snapshot
import extract_happy_hour_info

def get_happy_hour_details(restaurant_urls):
    all_results = {}
    for base_url in restaurant_urls:
        try:
            # Get response from Tavily
            response = tavily_web_crawl(base_url)
            # Extract all URLs we can find from the response
            urls = extract_urls_from_tavily(response) 
            context = {}
            # For each URL that Tavily found
            for url in urls:
                if is_img_or_pdf(url):
                    # Run OCR if it is an image or PDF
                    context[url] = extract_data_ocr(url)
                else:
                    # Otherwise, transform it into a PDF and run OCR
                    new_url = create_snapshot(url)
                    context[new_url] = extract_data_ocr(new_url)  
            # Get LLM interpretation of the context
            interpretation = extract_happy_hour_info(context)
            all_results[base_url] = {
                "context": context,
                "interpretation": interpretation,
            }          
        except Exception as e:
            print("Error processing {base_url}: {e}")
    return all_results