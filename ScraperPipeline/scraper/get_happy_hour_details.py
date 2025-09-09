from utils.is_img_or_pdf import is_img_or_pdf
from scraper.extract_urls_from_tavily import extract_urls_from_tavily
from scraper.tavily_web_crawl import tavily_web_crawl
from scraper.extract_data_ocr import extract_data_ocr
from scraper.create_snapshot import create_snapshot
from scraper.extract_happy_hour_info import extract_happy_hour_info

def get_happy_hour_details(restaurant_urls):
    all_results = {}
    for base_url in restaurant_urls:
        print(f"\nProcessing base_url: {base_url}")
        try:
            # Get response from Tavily
            response = tavily_web_crawl(base_url)
            # Extract all URLs we can find from the response
            urls = extract_urls_from_tavily(response, base_url)
            context = {}
            # For each URL that Tavily found
            for url in urls:
                if is_img_or_pdf(url):
                    context[url] = extract_data_ocr(url)
                else:
                    new_url = create_snapshot(url)
                    # TODO: Uncomment this line when implemented
                    # context[new_url] = extract_data_ocr(new_url)
            # Get LLM interpretation of the context
            interpretation = extract_happy_hour_info(context)
            all_results[base_url] = {
                "context": context,
                "interpretation": interpretation,
            }          
        except Exception as e:
            import traceback
            print(f"Error processing {base_url}: {e}")
            traceback.print_exc()

    return all_results
