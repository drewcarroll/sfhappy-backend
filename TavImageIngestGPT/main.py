import json
import TavImageIngestGPT.scraper.get_happy_hour_details as get_happy_hour_details

if __name__ == "__main__":
    restaurant_urls = [
        "https://www.bondeliresf.com/",
        "https://www.delarosasf.com/",
    ]
    results = get_happy_hour_details(restaurant_urls)
    print(json.dumps(results, indent=2, ensure_ascii=False))