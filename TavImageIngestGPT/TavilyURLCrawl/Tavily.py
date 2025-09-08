from tavily import TavilyClient
#runs Tavily web crawl on given base URL


client = TavilyClient("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")
def TavilyWebCrawl(base_url):
    response = client.crawl(
                url=base_url,
                instructions="Get all pages related to happy hour.",
                extract_depth="advanced"
            )
    return response