from tavily import TavilyClient

client = TavilyClient("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")
def tavily_web_crawl(base_url):
    response = client.crawl(
        url=base_url,
        instructions="Get all pages related to happy hour.",
        extract_depth="advanced"
    )
    return response