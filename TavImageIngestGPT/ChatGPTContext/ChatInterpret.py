from openai import OpenAI
from dotenv import load_dotenv
import os
import re
import json

load_dotenv()
client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))

def extract_happy_hour_info(raw_text: str) -> dict:
    """
    Calls ChatGPT (cheap model) to interpret raw menu text
    and return structured happy hour info.
    
    Args:
        raw_text (str): Text content from a menu or PDF crawl.
    
    Returns:
        dict: { "happy_hour_times": "...", "happy_hour_items": [...] }
    """

    prompt = f"""
    You are given text from a restaurant website or menu. 
    Extract:
    1. The times when Happy Hour is available (days & times).
    2. The food and drink specials offered during Happy Hour.

    Text:
    {raw_text}

    Return your answer in strict JSON format:
    {{
      "happy_hour_times": "<string, e.g. Mon-Fri 4-6pm>",
      "happy_hour_items": ["item 1", "item 2", "..."]
    }}
    """

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0
    )

    content = response.choices[0].message.content.strip()

    # ✅ Remove Markdown code fences if present
    if content.startswith("```"):
        content = re.sub(r"^```[a-zA-Z]*\n", "", content)
        content = re.sub(r"\n```$", "", content)

    try:
        parsed = json.loads(content)
    except Exception:
        parsed = {"happy_hour_times": None, "happy_hour_items": [], "raw_output": content}

    return parsed
