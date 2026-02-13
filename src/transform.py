import csv
import json
import os
from datetime import datetime, timezone
from pathlib import Path


# Issues observed in raw data (at least five):
# 1) Nested and non-tabular fields (e.g., categories, screenshots, histogram).
# 2) Mixed data types (e.g., installs is a string like "1,000,000+").
# 3) Missing or null values in multiple fields (e.g., developerAddress, video).
# 4) Timestamps can appear in different formats and may be invalid.
# 5) Reviews may contain app ids unknown in apps metadata.
# 6) Some fields can be empty or inconsistent across files (schema drift).
# 7) Potential duplicates and inconsistent identifiers across apps/reviews.


RAW_DIR = Path("data/raw")
PROCESSED_DIR = Path("data/processed")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

RAW_APPS_PATH = Path(os.environ.get("RAW_APPS_PATH", str(RAW_DIR / "apps.jsonl")))
RAW_REVIEWS_PATHS = os.environ.get("RAW_REVIEWS_PATHS", str(RAW_DIR / "reviews.jsonl"))
REVIEWS_INPUTS = [Path(p.strip()) for p in RAW_REVIEWS_PATHS.split(",") if p.strip()]

APPS_OUT = PROCESSED_DIR / "apps.csv"
REVIEWS_OUT = PROCESSED_DIR / "reviews.csv"
QUALITY_REPORT_OUT = PROCESSED_DIR / "quality_report.json"

NULL_TOKENS = {"", "null", "none", "nan", "na", "n/a", "<na>"}
TIMESTAMP_FORMATS = [
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%d",
    "%Y/%m/%d",
    "%m/%d/%Y %H:%M:%S",
    "%m/%d/%Y %H:%M",
    "%m/%d/%Y",
    "%d/%m/%Y %H:%M:%S",
    "%d/%m/%Y %H:%M",
    "%d/%m/%Y",
]

APP_FIELDNAMES = [
    "appId",
    "title",
    "developer",
    "score",
    "ratings",
    "installs",
    "genre",
    "price",
]

REVIEW_FIELDNAMES = [
    "app_id",
    "app_name",
    "reviewId",
    "userName",
    "score",
    "content",
    "thumbsUpCount",
    "at",
]


def init_stats():
    return {
        "inputs": {
            "apps_path": str(RAW_APPS_PATH),
            "reviews_paths": [str(p) for p in REVIEWS_INPUTS],
        },
        "apps": {
            "input_rows": 0,
            "output_rows": 0,
            "duplicate_app_ids": 0,
            "missing_app_id": 0,
            "missing_title": 0,
            "invalid_score": 0,
            "invalid_ratings": 0,
            "invalid_installs": 0,
            "invalid_price": 0,
        },
        "reviews": {
            "input_rows": 0,
            "output_rows": 0,
            "duplicate_review_ids": 0,
            "missing_review_id": 0,
            "missing_app_id": 0,
            "unknown_app_id": 0,
            "invalid_score": 0,
            "invalid_timestamp": 0,
            "empty_content": 0,
        },
    }


def normalize_null(value):
    if value is None:
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        if cleaned.lower() in NULL_TOKENS:
            return None
        return cleaned
    return value


def canonicalize_row(row):
    result = {}
    for key, value in row.items():
        if key is None:
            continue
        normalized_key = str(key).strip()
        result[normalized_key] = value
        lowered = normalized_key.lower()
        if lowered not in result:
            result[lowered] = value
    return result


def coalesce(obj, keys):
    for key in keys:
        if key in obj:
            value = normalize_null(obj.get(key))
            if value is not None:
                return value
    return None


def parse_int(value):
    value = normalize_null(value)
    if value is None:
        return None, False
    if isinstance(value, bool):
        return int(value), False
    if isinstance(value, int):
        return value, False
    if isinstance(value, float):
        if value != value:
            return None, False
        return int(value), False
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("+", "").strip()
        if cleaned == "":
            return None, False
        try:
            return int(float(cleaned)), False
        except ValueError:
            return None, True
    return None, True


def parse_float(value):
    value = normalize_null(value)
    if value is None:
        return None, False
    if isinstance(value, bool):
        return float(int(value)), False
    if isinstance(value, (int, float)):
        if isinstance(value, float) and value != value:
            return None, False
        return float(value), False
    if isinstance(value, str):
        cleaned = value.replace(",", "").replace("+", "").strip()
        if cleaned == "":
            return None, False
        try:
            return float(cleaned), False
        except ValueError:
            return None, True
    return None, True


def parse_rating(value):
    score, invalid = parse_int(value)
    if score is None:
        return None, invalid
    if score < 1 or score > 5:
        return None, True
    return score, False


def normalize_timestamp(value):
    value = normalize_null(value)
    if value is None:
        return None, False
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M:%S"), False
    if isinstance(value, (int, float)):
        timestamp = float(value)
        if timestamp > 10_000_000_000:
            timestamp = timestamp / 1000.0
        try:
            dt = datetime.utcfromtimestamp(timestamp)
            return dt.strftime("%Y-%m-%d %H:%M:%S"), False
        except (OverflowError, OSError, ValueError):
            return None, True
    if isinstance(value, str):
        candidate = value.replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(candidate)
            if dt.tzinfo is not None:
                dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
            return dt.strftime("%Y-%m-%d %H:%M:%S"), False
        except ValueError:
            pass
        for fmt in TIMESTAMP_FORMATS:
            try:
                dt = datetime.strptime(value, fmt)
                return dt.strftime("%Y-%m-%d %H:%M:%S"), False
            except ValueError:
                continue
    return None, True


def read_jsonl(path):
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def read_json(path):
    with path.open(encoding="utf-8") as f:
        payload = json.load(f)
    if isinstance(payload, list):
        for row in payload:
            yield row
        return
    if isinstance(payload, dict):
        yield payload
        return
    raise ValueError(f"Unsupported JSON payload in {path}")


def read_csv_rows(path):
    with path.open(encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def read_raw_rows(path):
    if not path.exists():
        raise FileNotFoundError(f"Raw input not found: {path}")
    suffix = path.suffix.lower()
    if suffix == ".jsonl":
        yield from read_jsonl(path)
        return
    if suffix == ".json":
        yield from read_json(path)
        return
    if suffix == ".csv":
        yield from read_csv_rows(path)
        return
    raise ValueError(f"Unsupported raw format: {path}")


def normalize_app_row(obj, stats):
    row = canonicalize_row(obj)
    app_id = coalesce(
        row,
        [
            "appId",
            "app_id",
            "appid",
            "id",
            "packageName",
            "package_name",
            "package",
        ],
    )
    title = coalesce(row, ["title", "app_name", "appName", "name", "app_title"])
    developer = coalesce(
        row, ["developer", "developerName", "developer_name", "publisher", "company"]
    )
    score, bad_score = parse_float(
        coalesce(row, ["score", "rating", "stars", "average_rating", "avg_rating"])
    )
    ratings, bad_ratings = parse_int(
        coalesce(row, ["ratings", "ratingCount", "rating_count", "ratings_count"])
    )
    installs, bad_installs = parse_int(
        coalesce(
            row,
            ["installs", "minInstalls", "min_installs", "installCount", "downloads"],
        )
    )
    genre = coalesce(row, ["genre", "category", "primaryGenre", "app_category"])
    price, bad_price = parse_float(coalesce(row, ["price", "price_usd", "priceUSD"]))

    if bad_score:
        stats["apps"]["invalid_score"] += 1
    if bad_ratings:
        stats["apps"]["invalid_ratings"] += 1
    if bad_installs:
        stats["apps"]["invalid_installs"] += 1
    if bad_price:
        stats["apps"]["invalid_price"] += 1

    if app_id is None:
        stats["apps"]["missing_app_id"] += 1
    if title is None:
        stats["apps"]["missing_title"] += 1

    return {
        "appId": app_id,
        "title": title,
        "developer": developer,
        "score": score,
        "ratings": ratings,
        "installs": installs,
        "genre": genre,
        "price": price,
    }


def normalize_review_row(obj, app_name_by_id, stats):
    row = canonicalize_row(obj)
    app_id = coalesce(
        row,
        [
            "appId",
            "app_id",
            "appid",
            "app",
            "packageName",
            "package_name",
            "package",
            "app_package",
        ],
    )
    app_name = app_name_by_id.get(app_id) or coalesce(
        row, ["app_name", "appName", "appTitle", "title", "app_title"]
    )
    review_id = coalesce(row, ["reviewId", "review_id", "reviewid", "id", "comment_id"])
    user_name = coalesce(
        row, ["userName", "user_name", "author", "user", "authorName"]
    )
    score, bad_score = parse_rating(
        coalesce(row, ["score", "rating", "stars", "review_score"])
    )
    content = coalesce(
        row, ["content", "text", "review", "reviewText", "review_text", "comment", "body"]
    )
    thumbs, _ = parse_int(
        coalesce(row, ["thumbsUpCount", "thumbs_up_count", "thumbsUp", "likes"])
    )
    at, bad_timestamp = normalize_timestamp(
        coalesce(row, ["at", "date", "created_at", "timestamp", "time", "review_date"])
    )

    if review_id is None:
        stats["reviews"]["missing_review_id"] += 1
    if app_id is None:
        stats["reviews"]["missing_app_id"] += 1
    elif app_id not in app_name_by_id:
        stats["reviews"]["unknown_app_id"] += 1
    if bad_score:
        stats["reviews"]["invalid_score"] += 1
    if bad_timestamp:
        stats["reviews"]["invalid_timestamp"] += 1
    if not content:
        stats["reviews"]["empty_content"] += 1

    return {
        "app_id": app_id,
        "app_name": app_name,
        "reviewId": review_id,
        "userName": user_name,
        "score": score,
        "content": content,
        "thumbsUpCount": thumbs,
        "at": at,
    }


def transform_apps(stats):
    apps_rows = []
    app_name_by_id = {}
    seen_app_ids = set()

    for obj in read_raw_rows(RAW_APPS_PATH):
        stats["apps"]["input_rows"] += 1
        row = normalize_app_row(obj, stats)
        app_id = row.get("appId")

        if app_id:
            if app_id in seen_app_ids:
                stats["apps"]["duplicate_app_ids"] += 1
                continue
            seen_app_ids.add(app_id)
            if row.get("title") and app_id not in app_name_by_id:
                app_name_by_id[app_id] = row["title"]

        apps_rows.append(row)

    apps_rows.sort(key=lambda r: ((r["appId"] or ""), (r["title"] or "")))
    stats["apps"]["output_rows"] = len(apps_rows)

    with APPS_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=APP_FIELDNAMES)
        writer.writeheader()
        writer.writerows(apps_rows)

    print(
        f"Wrote {APPS_OUT} ({len(apps_rows)} rows, "
        f"{stats['apps']['duplicate_app_ids']} duplicate app_ids skipped)"
    )
    return app_name_by_id


def transform_reviews(app_name_by_id, stats):
    reviews_rows = []
    seen_review_ids = set()

    for path in REVIEWS_INPUTS:
        for obj in read_raw_rows(path):
            stats["reviews"]["input_rows"] += 1
            row = normalize_review_row(obj, app_name_by_id, stats)
            review_id = row.get("reviewId")

            if review_id:
                if review_id in seen_review_ids:
                    stats["reviews"]["duplicate_review_ids"] += 1
                    continue
                seen_review_ids.add(review_id)

            if not row.get("app_name") and row.get("app_id") in app_name_by_id:
                row["app_name"] = app_name_by_id[row["app_id"]]

            reviews_rows.append(row)

    reviews_rows.sort(
        key=lambda r: ((r["app_id"] or ""), (r["reviewId"] or ""), (r["at"] or ""))
    )
    stats["reviews"]["output_rows"] = len(reviews_rows)

    with REVIEWS_OUT.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=REVIEW_FIELDNAMES)
        writer.writeheader()
        writer.writerows(reviews_rows)
    print(
        f"Wrote {REVIEWS_OUT} ({len(reviews_rows)} rows, "
        f"{stats['reviews']['duplicate_review_ids']} duplicate reviewIds skipped)"
    )


def write_quality_report(stats):
    stats["generated_at_utc"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    with QUALITY_REPORT_OUT.open("w", encoding="utf-8") as f:
        json.dump(stats, f, indent=2, ensure_ascii=False)
    print(f"Wrote {QUALITY_REPORT_OUT}")


def main():
    print("Starting full refresh transformation")
    print(f"Apps input: {RAW_APPS_PATH}")
    print(f"Reviews inputs: {[str(p) for p in REVIEWS_INPUTS]}")

    stats = init_stats()
    app_name_by_id = transform_apps(stats)
    transform_reviews(app_name_by_id, stats)
    write_quality_report(stats)


if __name__ == "__main__":
    main()
