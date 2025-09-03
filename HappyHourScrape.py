import os
import math
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.write_concern import WriteConcern

# ----------------- Config -----------------
DEBUG_FILE = "debug.csv"
CREDITS = {"nearby": 0, "details": 0}

# ----------------- Logging -----------------
def log(msg: str) -> None:
    print(f"[LOG] {msg}")

# ----------------- MongoDB -----------------
"""
load_dotenv()

MONGO_USER = os.getenv("MONGO_USER")
MONGO_PASS = os.getenv("MONGO_PASS")

if not (MONGO_USER and MONGO_PASS):
    raise RuntimeError("❌ MONGO_USER or MONGO_PASS not found in .env")

uri = f"mongodb+srv://{MONGO_USER}:{MONGO_PASS}@happyhoursf.2bapz.mongodb.net/?retryWrites=true&w=majority&appName=happyhoursf"
client = MongoClient(uri)

db = client["happyhour"]
restaurants = db["restaurants_raw"]
# restaurants_wc = restaurants.with_options(write_concern=WriteConcern(w=1, j=False))
"""
# ----------------- Google Places v1 -----------------

load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise RuntimeError("❌ GOOGLE_API_KEY not found in .env")

FIELDS_MASK = (
    "places.id,"
    "places.displayName,"
    "places.location,"
    "places.primaryType,"
    "places.businessStatus,"
    "places.regularOpeningHours,"
    "places.regularSecondaryOpeningHours"
)

DETAIL_FIELDS = (
    "id,"
    "displayName,"
    "location,"              
    "formattedAddress,"
    "primaryType,"
    "types,"
    "websiteUri,"
    "businessStatus,"
    "nationalPhoneNumber,"
    "rating,"
    "userRatingCount,"
    "priceLevel,"
    "photos,"
    "editorialSummary,"
    "regularOpeningHours,"
    "regularSecondaryOpeningHours"
)

def nearby_search(lat: float, lng: float, radius_m: int):
    global CREDITS
    url = "https://places.googleapis.com/v1/places:searchNearby"
    headers = {
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": FIELDS_MASK,
        "Content-Type": "application/json",
    }
    body = {
        "includedTypes": ["restaurant", "bar"],
        "locationRestriction": {
            "circle": {"center": {"latitude": lat, "longitude": lng}, "radius": radius_m}
        }
    }
    CREDITS["nearby"] += 1
    resp = requests.post(url, headers=headers, json=body)
    try:
        return resp.json().get("places", [])
    except json.JSONDecodeError:
        log(f"[WARN] nearby_search JSON decode failed: {resp.text[:200]}")
        return []

def get_place_details(place_id: str):
    global CREDITS
    url = f"https://places.googleapis.com/v1/places/{place_id}"
    headers = {
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": DETAIL_FIELDS,
    }
    CREDITS["details"] += 1
    resp = requests.get(url, headers=headers)
    try:
        return resp.json()
    except json.JSONDecodeError:
        log(f"[WARN] get_place_details JSON decode failed for {place_id}: {resp.text[:200]}")
        return {}

# ----------------- Persistence -----------------
def save_progress_bulk(data_rows, batch_size=200):
    ops, flat_rows = [], []
    for d in data_rows:
        flat = {
            "_id": d.get("id"),
            "name": (d.get("displayName") or {}).get("text"),
            "address": d.get("formattedAddress"),
            "lat": (d.get("location") or {}).get("latitude"),
            "lng": (d.get("location") or {}).get("longitude"),
            "rating": d.get("rating"),
            "userRatingCount": d.get("userRatingCount"),
            "priceLevel": d.get("priceLevel"),
            "phone": d.get("nationalPhoneNumber"),
            "website": d.get("websiteUri"),
            "categories": d.get("types"),
            "primaryType": d.get("primaryType"),
            "regularHours": d.get("regularOpeningHours"),
            "secondaryHours": d.get("regularSecondaryOpeningHours"),
            "editorialSummary": (d.get("editorialSummary") or {}).get("text"),
            "businessStatus": d.get("businessStatus"),
            "photos": d.get("photos"),
            "raw_json": d,
        }

        if not flat["_id"]:
            log("⏭️ Skipping doc with missing id")
            continue

        ops.append(UpdateOne({"_id": flat["_id"]}, {"$set": flat}, upsert=True))
        flat_rows.append(flat)

        #if len(ops) >= batch_size:
           # restaurants_wc.bulk_write(ops, ordered=False)
            #ops.clear()

    #if ops:
       # restaurants_wc.bulk_write(ops, ordered=False)

    if flat_rows:
        pd.DataFrame(flat_rows).to_csv(DEBUG_FILE, index=False)
    log(f"🍜 Upserts this batch: {len(flat_rows)} | 💾 CSV: {DEBUG_FILE}")

# ----------------- Search (quad split on 20-cap) -----------------
def search_box(ne_lat, ne_lng, sw_lat, sw_lng, seen, data_rows, min_size_m=200):
    lat_m = (ne_lat - sw_lat) * 110_574
    lng_m = (ne_lng - sw_lng) * (111_320 * math.cos(math.radians((ne_lat + sw_lat) / 2.0)))
    radius = int(math.sqrt(lat_m**2 + lng_m**2) / 2)
    center_lat = (ne_lat + sw_lat) / 2.0
    center_lng = (ne_lng + sw_lng) / 2.0

    log(f"▶️ Box NE=({ne_lat:.5f},{ne_lng:.5f}) SW=({sw_lat:.5f},{sw_lng:.5f}) r≈{radius}m")
    results = nearby_search(center_lat, center_lng, radius)

    # Subdivide if we hit the 20-result page cap and the box isn't tiny
    if len(results) >= 20 and max(lat_m, lng_m) > min_size_m:
        log("⚠️ Got 20 results → subdividing")
        mid_lat = (ne_lat + sw_lat) / 2.0
        mid_lng = (ne_lng + sw_lng) / 2.0
        search_box(ne_lat, ne_lng, mid_lat, mid_lng, seen, data_rows, min_size_m)
        search_box(ne_lat, mid_lng, mid_lat, sw_lng, seen, data_rows, min_size_m)
        search_box(mid_lat, ne_lng, sw_lat, mid_lng, seen, data_rows, min_size_m)
        search_box(mid_lat, mid_lng, sw_lat, sw_lng, seen, data_rows, min_size_m)
        return

    # Leaf: pull details for any unseen places

    GOOD_STATUSES = {"OPERATIONAL"}  # keep near top if you prefer

    for r in results:
        pid = r.get("id")
        if not pid or pid in seen:
            continue
        seen.add(pid)
    
        status_ok = (r.get("businessStatus"))
        if status_ok not in GOOD_STATUSES:
            continue

        sec_hours = r.get("regularSecondaryOpeningHours") or []

        # Only proceed if ANY section is HAPPY_HOUR
        has_hh = False
        for s in sec_hours:
            sec_type = (s.get("secondaryHoursType") or s.get("type") or "Unknown").upper()
            if sec_type == "HAPPY_HOUR":
                has_hh = True
                break
        
        if not has_hh:
            continue

        # Only now fetch details (spend the credit)

        # Details-level status gate
        details = get_place_details(pid)
        if not details:
            continue

        data_rows.append(details)
        name = (details.get("displayName") or {}).get("text", "Unknown")
        log(f"✔️ Saved: {name}")

        if len(data_rows) >= 200:
            save_progress_bulk(data_rows)
            data_rows.clear()

        

# ----------------- Main -----------------
def main(ne_lat, ne_lng, sw_lat, sw_lng):
    seen = set()
    #seen = load_seen()

    data_rows = []

    search_box(ne_lat, ne_lng, sw_lat, sw_lng, seen, data_rows)

    # Final flush
    if data_rows:
        save_progress_bulk(data_rows)

    log("✅ Finished.")
    log("--- CREDIT USAGE ---")
    log(f"Nearby calls: {CREDITS['nearby']} | Detail calls: {CREDITS['details']}")

if __name__ == "__main__":
    # Example: SLO bounds (adjust as needed)
    NE_LAT, NE_LNG = 37.8108, -122.3569
    SW_LAT, SW_LNG = 37.7068, -122.5158
    main(NE_LAT, NE_LNG, SW_LAT, SW_LNG)