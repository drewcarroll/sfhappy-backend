from scraper.extract_secondary_links import extract_secondary_links

def extract_urls_from_tavily(response, base_url):
    if not response or "results" not in response:
        return []

    urls = set()

    # Include base URL
    urls.add(base_url)

    # Add URLs explicitly in the response
    for item in response["results"]:
        if "url" in item:
            urls.add(item["url"])

    # Add extracted file/image links from results
    file_links = extract_secondary_links(response["results"], base_url)
    urls.update(file_links)

    return list(urls)