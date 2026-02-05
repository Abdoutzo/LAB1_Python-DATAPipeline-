import json
import time
from pathlib import Path

from google_play_scraper import app as gp_app
from google_play_scraper import reviews, search


RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)

APPS_OUT = RAW_DIR / "apps.jsonl"
REVIEWS_OUT = RAW_DIR / "reviews.jsonl"

# Tweak these as needed
QUERY = "ai note taking"
NUM_APPS = 40
SLEEP_SECONDS = 0.5
REVIEWS_LIMIT = 200
DEBUG_SINGLE_APP = False
TEST_APP_ID = "com.aisense.otter"


def fetch_app_ids():
    results = search(QUERY, n_hits=NUM_APPS)
    app_ids = [r["appId"] for r in results if "appId" in r]
    return list(dict.fromkeys(app_ids))  # preserve order, remove duplicates


def write_jsonl(path, rows):
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            # Convert non-JSON types (e.g., datetime) to strings for raw storage
            f.write(json.dumps(row, ensure_ascii=False, default=str) + "\n")


def main():
    app_ids = fetch_app_ids()
    if TEST_APP_ID not in app_ids:
        app_ids = [TEST_APP_ID] + app_ids
    if DEBUG_SINGLE_APP:
        app_ids = [TEST_APP_ID]
    print(f"Found {len(app_ids)} apps for query: {QUERY}")

    apps = []
    all_reviews = []

    for idx, app_id in enumerate(app_ids, start=1):
        print(f"[{idx}/{len(app_ids)}] Fetching app: {app_id}")
        try:
            meta = gp_app(app_id)
            apps.append(meta)
        except Exception as e:
            print(f"  Failed to fetch app metadata: {e}")
            continue

        try:
            reviews_batch, _ = reviews(
                app_id,
                lang="en",
                country="us",
                count=REVIEWS_LIMIT,
            )
            print(f"  Reviews fetched: {len(reviews_batch)} (limit {REVIEWS_LIMIT})")
            if reviews_batch:
                print(f"  Review keys sample: {list(reviews_batch[0].keys())}")
            for r in reviews_batch:
                r["appId"] = app_id
            all_reviews.extend(reviews_batch)
        except Exception as e:
            print(f"  Failed to fetch reviews: {type(e).__name__}: {e}")

        time.sleep(SLEEP_SECONDS)

    write_jsonl(APPS_OUT, apps)
    write_jsonl(REVIEWS_OUT, all_reviews)

    print(f"Wrote {len(apps)} apps to {APPS_OUT}")
    print(f"Wrote {len(all_reviews)} reviews to {REVIEWS_OUT}")


if __name__ == "__main__":
    main()
