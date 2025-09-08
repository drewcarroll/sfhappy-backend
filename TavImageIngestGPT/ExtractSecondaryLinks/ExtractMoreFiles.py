from tavily import TavilyClient
import re
from urllib.parse import urljoin

client = TavilyClient("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")

def extract_files_from_results(results, base_url, valid_exts):
    """
    Extract PDF/PNG/JPG/JPEG links from Tavily crawl results.

    Args:
        results (list): List of result dicts from Tavily crawl
        base_url (str): Base URL of the restaurant site
        valid_exts (tuple): Allowed file extensions (e.g. (".pdf", ".png"))

    Returns:
        list: Unique list of dicts with {file_url, source_page}
    """
    found_files = []
    
    print("this is result", results)
    for result in results:
        print("this is result in results", result)
        raw = result.get("raw_content", "")
        page_url = result.get("url", base_url)

        # go line by line
        for line in raw.splitlines():
            matches = re.findall(r'(https?://[^\s\)"]+|//[^\s\)"]+|/[^\s\)"]+)', line)

            for match in matches:
                if match.startswith("//"):        # protocol-relative
                    link = "https:" + match
                elif match.startswith("http"):    # absolute
                    link = match
                else:                             # relative
                    link = urljoin(base_url, match)

                # only keep valid extensions
                if link.lower().split("?")[0].endswith(valid_exts):
                    found_files.append(link)
                print("FoundFiles:", found_files)

    # ✅ Deduplicate by file_url
    unique_urls = list(dict.fromkeys(found_files))
    print("Unique URLs:", unique_urls)
    return unique_urls 