import re
from urllib.parse import urljoin

def extract_secondary_links(results, base_url):
    found_files = []
    for result in results:
        raw = result.get("raw_content", "")
        for line in raw.splitlines():
            matches = re.findall(r'(https?://[^\s\)"]+|//[^\s\)"]+|/[^\s\)"]+)', line)

            for match in matches:
                if match.startswith("//"):
                    link = "https:" + match
                elif match.startswith("http"):
                    link = match
                else:
                    link = urljoin(base_url, match)

                found_files.append(link)
                print("FoundFiles:", found_files)
    unique_urls = list(dict.fromkeys(found_files))
    print("Unique URLs:", unique_urls)
    return unique_urls
