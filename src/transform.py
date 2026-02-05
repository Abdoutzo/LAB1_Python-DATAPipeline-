import csv
import json
from datetime import datetime
from pathlib import Path


# Issues observed in raw data (at least five):
# 1) Nested and non-tabular fields (e.g., categories, screenshots, histogram).
# 2) Mixed data types (e.g., installs is a string like "1,000,000+").
# 3) Missing or null values in multiple fields (e.g., developerAddress, video).
# 4) Timestamps stored as strings without explicit timezone (e.g., review "at").
# 5) Reviews lack app name, only appId; requires join with apps dataset.
# 6) HTML / long text fields (descriptionHTML) are not analytics-friendly as-is.
# 7) Potential duplicates and inconsistent identifiers across apps/reviews.


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

APPS_IN = RAW_DIR / "apps.jsonl"
REVIEWS_IN = RAW_DIR / "reviews.jsonl"

APPS_OUT = PROCESSED_DIR / "apps.csv"
REVIEWS_OUT = PROCESSED_DIR / "reviews.csv"


def parse_int(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("+", "").strip()
        if cleaned == "":
            return None
        try:
            return int(cleaned)
        except ValueError:
            return None
    return None


def parse_float(value):
    if value is None:
        return None
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("+", "").strip()
        if cleaned == "":
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def normalize_timestamp(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S")
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            return value
    return str(value)


def read_jsonl(path):
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def transform_apps():
    apps_rows = []
    app_name_by_id = {}

    for obj in read_jsonl(APPS_IN):
        app_id = obj.get("appId")
        title = obj.get("title")
        developer = obj.get("developer")
        score = parse_float(obj.get("score"))
        ratings = parse_int(obj.get("ratings"))
        installs = parse_int(obj.get("installs"))
        genre = obj.get("genre")
        price = parse_float(obj.get("price"))

        if app_id:
            app_name_by_id[app_id] = title

        apps_rows.append(
            {
                "appId": app_id,
                "title": title,
                "developer": developer,
                "score": score,
                "ratings": ratings,
                "installs": installs,
                "genre": genre,
                "price": price,
            }
        )

    with APPS_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "appId",
                "title",
                "developer",
                "score",
                "ratings",
                "installs",
                "genre",
                "price",
            ],
        )
        writer.writeheader()
        writer.writerows(apps_rows)

    return app_name_by_id


def transform_reviews(app_name_by_id):
    reviews_rows = []
    for obj in read_jsonl(REVIEWS_IN):
        app_id = obj.get("appId")
        reviews_rows.append(
            {
                "app_id": app_id,
                "app_name": app_name_by_id.get(app_id),
                "reviewId": obj.get("reviewId"),
                "userName": obj.get("userName"),
                "score": parse_int(obj.get("score")),
                "content": obj.get("content"),
                "thumbsUpCount": parse_int(obj.get("thumbsUpCount")),
                "at": normalize_timestamp(obj.get("at")),
            }
        )

    with REVIEWS_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "app_id",
                "app_name",
                "reviewId",
                "userName",
                "score",
                "content",
                "thumbsUpCount",
                "at",
            ],
        )
        writer.writeheader()
        writer.writerows(reviews_rows)


def main():
    app_name_by_id = transform_apps()
    transform_reviews(app_name_by_id)
    print(f"Wrote {APPS_OUT}")
    print(f"Wrote {REVIEWS_OUT}")


if __name__ == "__main__":
    main()
