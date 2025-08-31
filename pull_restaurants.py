import requests
import pandas as pd
import math
import folium
import os
import json
from dotenv import load_dotenv

# --- Load API key ---
load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise RuntimeError("❌ GOOGLE_API_KEY not found in .env")

CREDITS = {"nearby": 0, "details": 0}
MAP = None
DATA_FILE = "restaurants_raw.csv"

# --- Logging ---
def log(msg): print(f"[LOG] {msg}")

# --- Folium map setup ---
def init_map(ne_lat, ne_lng, sw_lat, sw_lng):
    global MAP
    center_lat = (ne_lat + sw_lat) / 2
    center_lng = (ne_lng + sw_lng) / 2
    MAP = folium.Map(location=[center_lat, center_lng], zoom_start=14)
    folium.Rectangle(bounds=[(sw_lat, sw_lng), (ne_lat, ne_lng)],
                     color="blue", weight=2, fill=False).add_to(MAP)

def add_marker(lat, lng, name):
    global MAP
    folium.Marker(
        location=[lat, lng],
        popup=name,
        icon=folium.Icon(color="green", icon="cutlery", prefix="fa")
    ).add_to(MAP)

def save_map():
    global MAP
    MAP.save("debug_map.html")
    print("✅ Map saved to debug_map.html — open it in your browser.")

# --- Google Places (v1) ---
def nearby_search(lat, lng, radius):
    global CREDITS
    url = "https://places.googleapis.com/v1/places:searchNearby"
    headers = {
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": "places.id,places.displayName,places.location"
    }
    body = {
        "includedTypes": ["restaurant"],
        "locationRestriction": {
            "circle": {"center": {"latitude": lat, "longitude": lng}, "radius": radius}
        }
    }
    CREDITS["nearby"] += 1
    response = requests.post(url, headers=headers, json=body).json()
    return response.get("places", [])

def get_place_details(place_id):
    global CREDITS
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": "*"
    }
    CREDITS["details"] += 1
    return requests.get(url, headers=headers).json()

# --- Incremental save ---
def save_progress(data_rows):
    # Flatten useful fields + raw JSON
    flat_rows = []
    for d in data_rows:
        flat = {
            "id": d.get("id"),
            "name": d.get("displayName", {}).get("text"),
            "address": d.get("formattedAddress"),
            "lat": d.get("location", {}).get("latitude"),
            "lng": d.get("location", {}).get("longitude"),
            "rating": d.get("rating"),
            "userRatingCount": d.get("userRatingCount"),
            "priceLevel": d.get("priceLevel"),
            "phone": d.get("nationalPhoneNumber"),
            "website": d.get("websiteUri"),
            "categories": json.dumps(d.get("types")),
            "primaryType": d.get("primaryType"),
            "regularHours": json.dumps(d.get("regularOpeningHours")),
            "secondaryHours": json.dumps(d.get("regularSecondaryOpeningHours")),
            "editorialSummary": d.get("editorialSummary", {}).get("text"),
            "raw_json": json.dumps(d)
        }
        flat_rows.append(flat)

    df = pd.DataFrame(flat_rows)

    # Normalize missing values
    df = df.replace({"": None})

    df.to_csv(DATA_FILE, index=False)
    log(f"💾 Progress saved ({len(flat_rows)} places)")


# --- Exhaustive box subdivision ---
def search_box(ne_lat, ne_lng, sw_lat, sw_lng, seen, data_rows, min_size_m=200):
    # Box dimensions in meters
    lat_m = (ne_lat - sw_lat) * 110574
    lng_m = (ne_lng - sw_lng) * (111320 * math.cos(math.radians((ne_lat+sw_lat)/2)))
    radius = int(math.sqrt(lat_m**2 + lng_m**2) / 2)

    center_lat = (ne_lat + sw_lat) / 2
    center_lng = (ne_lng + sw_lng) / 2

    log(f"▶️ Searching box NE=({ne_lat:.4f},{ne_lng:.4f}), SW=({sw_lat:.4f},{sw_lng:.4f}), r≈{radius}m")
    results = nearby_search(center_lat, center_lng, radius)

    if len(results) >= 20 and max(lat_m, lng_m) > min_size_m:
        log(f"⚠️ Hit 20 results → subdividing box")
        mid_lat = (ne_lat + sw_lat) / 2
        mid_lng = (ne_lng + sw_lng) / 2
        search_box(ne_lat, ne_lng, mid_lat, mid_lng, seen, data_rows, min_size_m)
        search_box(ne_lat, mid_lng, mid_lat, sw_lng, seen, data_rows, min_size_m)
        search_box(mid_lat, ne_lng, sw_lat, mid_lng, seen, data_rows, min_size_m)
        search_box(mid_lat, mid_lng, sw_lat, sw_lng, seen, data_rows, min_size_m)
    else:
        for r in results:
            pid = r.get("id")
            if pid and pid not in seen:
                seen.add(pid)
                details = get_place_details(pid)
                data_rows.append(details)
                name = details.get("displayName", {}).get("text", "Unknown")
                loc = details.get("location", {})
                if loc.get("latitude") and loc.get("longitude"):
                    add_marker(loc["latitude"], loc["longitude"], name)
                log(f"✔️ Saved: {name}")
        save_progress(data_rows)

# --- Main ---
def main(ne_lat, ne_lng, sw_lat, sw_lng):
    seen, data_rows = set(), []

    init_map(ne_lat, ne_lng, sw_lat, sw_lng)
    search_box(ne_lat, ne_lng, sw_lat, sw_lng, seen, data_rows)

    log(f"✅ Finished. Total {len(data_rows)} places saved to {DATA_FILE}")
    log(f"--- CREDIT USAGE ---")
    log(f"Nearby calls: {CREDITS['nearby']} | Detail calls: {CREDITS['details']}")
    save_map()

if __name__ == "__main__":
    NE_LAT, NE_LNG = 35.315387, -120.721626
    SW_LAT, SW_LNG = 35.229789, -120.622404
    main(NE_LAT, NE_LNG, SW_LAT, SW_LNG)
