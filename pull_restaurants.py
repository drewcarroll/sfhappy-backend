import os
import math
import json
import requests
import pandas as pd
from dotenv import load_dotenv
from pymongo import MongoClient, UpdateOne
from pymongo.write_concern import WriteConcern
from pymongo.errors import OperationFailure

# ----------------- Config -----------------
DEBUG_FILE = "debug.csv"
CREDITS = {"nearby": 0, "details": 0}

# ----------------- Logging -----------------
def log(msg: str) -> None:
    print(f"[LOG] {msg}")

# ----------------- MongoDB -----------------
client = MongoClient("mongodb+srv://drew:drew@happyhoursf.2bapz.mongodb.net/?retryWrites=true&w=majority&appName=happyhoursf")
db = client["happyhour"]
restaurants = db["restaurants_raw"]
restaurants_wc = restaurants.with_options(write_concern=WriteConcern(w=1, j=False))  # faster ingestion

def ensure_indexes():
    """Create only what's necessary."""
    try:
        # You upsert with _id = place.id, so no need for a separate unique index on id.
        restaurants.create_index("photos.name", name="photos_name_idx")
    except OperationFailure as e:
        log(f"[WARN] Index creation issue: {e}")

def load_seen():
    """Return IDs we've already stored to skip duplicates on reruns."""
    return set(x["_id"] for x in restaurants.find({}, {"_id": 1}))

# ----------------- Google Places v1 -----------------
load_dotenv()
GOOGLE_API_KEY = os.getenv("GOOGLE_API_KEY")
if not GOOGLE_API_KEY:
    raise RuntimeError("❌ GOOGLE_API_KEY not found in .env")

NEARBY_FIELDS = "places.id,places.displayName,places.location"
DETAIL_FIELDS = "*"  # pull entire payload for raw_json

def nearby_search(lat: float, lng: float, radius_m: int):
    global CREDITS
    url = "https://places.googleapis.com/v1/places:searchNearby"
    headers = {
        "X-Goog-Api-Key": GOOGLE_API_KEY,
        "X-Goog-FieldMask": NEARBY_FIELDS,
        "Content-Type": "application/json",
    }
    body = {
        "includedTypes": ["restaurant"],
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
    """Bulk upsert minimal indexed fields + full raw_json. Photos stored raw via d.get('photos')."""
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
            "photos": d.get("photos"),   # raw photo objects, untouched
            "raw_json": d,               # entire payload, untouched
        }

        if not flat["_id"]:
            log("⏭️ Skipping doc with missing id")
            continue

        ops.append(UpdateOne({"_id": flat["_id"]}, {"$set": flat}, upsert=True))
        flat_rows.append(flat)

        if len(ops) >= batch_size:
            restaurants_wc.bulk_write(ops, ordered=False)
            ops.clear()

    if ops:
        restaurants_wc.bulk_write(ops, ordered=False)

    if flat_rows:
        # Light snapshot for eyeballing results
        pd.DataFrame(flat_rows).to_csv(DEBUG_FILE, index=False)
    log(f"🍜 Upserts this batch: {len(flat_rows)} | 💾 CSV: {DEBUG_FILE}")

# ----------------- Search (quad split on 20-cap) -----------------
def search_box(ne_lat, ne_lng, sw_lat, sw_lng, seen, data_rows, min_size_m=200):
    # approximate meters per degree
    lat_m = (ne_lat - sw_lat) * 110_574
    lng_m = (ne_lng - sw_lng) * (111_320 * math.cos(math.radians((ne_lat + sw_lat) / 2.0)))
    # circle radius ~ half diagonal of the box
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
    for r in results:
        pid = r.get("id")
        if not pid or pid in seen:
            continue
        seen.add(pid)
        details = get_place_details(pid)
        if not details:
            continue
        if details.get("businessStatus") == "CLOSED_PERMANENTLY":
            continue
        data_rows.append(details)
        name = (details.get("displayName") or {}).get("text", "Unknown")
        log(f"✔️ Saved: {name}")

    # Periodic flush
    if len(data_rows) >= 200:
        save_progress_bulk(data_rows)
        data_rows.clear()

# ----------------- Main -----------------
def main(ne_lat, ne_lng, sw_lat, sw_lng):
    ensure_indexes()
    seen = load_seen()
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
    NE_LAT, NE_LNG = 35.315387, -120.721626
    SW_LAT, SW_LNG = 35.229789, -120.622404
    main(NE_LAT, NE_LNG, SW_LAT, SW_LNG)
