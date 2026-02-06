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
REVIEWS_PER_PAGE = 200
# Set to None to fetch all available review pages.
MAX_REVIEWS_PER_APP = 1000
DEBUG_SINGLE_APP = False
TEST_APP_ID = "com.aisense.otter"


def fetch_app_ids():
    results = search(QUERY, n_hits=NUM_APPS)
    app_ids = [r["appId"] for r in results if "appId" in r]
    return list(dict.fromkeys(app_ids))  # preserve order, remove duplicates


def append_jsonl(path, rows):
    with path.open("a", encoding="utf-8") as f:
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

    APPS_OUT.write_text("", encoding="utf-8")
    REVIEWS_OUT.write_text("", encoding="utf-8")

    apps_written = 0
    reviews_written_total = 0

    for idx, app_id in enumerate(app_ids, start=1):
        print(f"[{idx}/{len(app_ids)}] Fetching app: {app_id}")
        try:
            meta = gp_app(app_id)
            append_jsonl(APPS_OUT, [meta])
            apps_written += 1
        except Exception as e:
            print(f"  Failed to fetch app metadata: {e}")
            continue

        try:
            app_reviews = 0
            token = None
            page = 0
            while True:
                remaining = None
                if MAX_REVIEWS_PER_APP is not None:
                    remaining = MAX_REVIEWS_PER_APP - app_reviews
                    if remaining <= 0:
                        break
                count = (
                    REVIEWS_PER_PAGE
                    if remaining is None
                    else min(REVIEWS_PER_PAGE, remaining)
                )
                reviews_batch, token = reviews(
                    app_id,
                    lang="en",
                    country="us",
                    count=count,
                    continuation_token=token,
                )
                if not reviews_batch:
                    break
                page += 1
                if reviews_batch:
                    print(
                        f"  Reviews page {page}: {len(reviews_batch)} "
                        f"(total {app_reviews + len(reviews_batch)})"
                    )
                    if app_reviews == 0:
                        print(f"  Review keys sample: {list(reviews_batch[0].keys())}")
                for r in reviews_batch:
                    r["appId"] = app_id
                append_jsonl(REVIEWS_OUT, reviews_batch)
                app_reviews += len(reviews_batch)
                if not token:
                    break
            reviews_written_total += app_reviews
        except Exception as e:
            print(f"  Failed to fetch reviews: {type(e).__name__}: {e}")

        time.sleep(SLEEP_SECONDS)

    print(f"Wrote {apps_written} apps to {APPS_OUT}")
    print(f"Wrote {reviews_written_total} reviews to {REVIEWS_OUT}")


if __name__ == "__main__":
    main()
