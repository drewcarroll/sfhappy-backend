import sys, os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import json
from scraper.get_happy_hour_details import get_happy_hour_details

if __name__ == "__main__":
    restaurant_urls = [
        "https://www.bondeliresf.com/",
        "https://www.delarosasf.com/",
    ]
    results = get_happy_hour_details(restaurant_urls)
    print(json.dumps(results, indent=2, ensure_ascii=False))
