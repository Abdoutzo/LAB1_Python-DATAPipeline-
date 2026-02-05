import csv
from collections import defaultdict
from datetime import datetime
from pathlib import Path


PROCESSED_DIR = Path("data/processed")
REVIEWS_IN = PROCESSED_DIR / "reviews.csv"

APP_KPI_OUT = PROCESSED_DIR / "app_kpis.csv"
DAILY_KPI_OUT = PROCESSED_DIR / "daily_kpis.csv"


def parse_ts(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def load_reviews():
    with REVIEWS_IN.open(encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            yield row


def compute_app_kpis(rows):
    stats = {}

    for r in rows:
        app_id = r.get("app_id")
        app_name = r.get("app_name")
        score = r.get("score")
        at = parse_ts(r.get("at"))

        if app_id not in stats:
            stats[app_id] = {
                "app_id": app_id,
                "app_name": app_name,
                "reviews_count": 0,
                "score_sum": 0.0,
                "low_count": 0,
                "first_review_date": None,
                "last_review_date": None,
            }

        s = stats[app_id]
        s["reviews_count"] += 1

        try:
            score_val = int(score)
            s["score_sum"] += score_val
            if score_val <= 2:
                s["low_count"] += 1
        except Exception:
            pass

        if at:
            if s["first_review_date"] is None or at < s["first_review_date"]:
                s["first_review_date"] = at
            if s["last_review_date"] is None or at > s["last_review_date"]:
                s["last_review_date"] = at

    out_rows = []
    for app_id, s in stats.items():
        count = s["reviews_count"]
        avg = s["score_sum"] / count if count else None
        low_pct = (s["low_count"] / count) * 100 if count else None
        out_rows.append(
            {
                "app_id": app_id,
                "app_name": s["app_name"],
                "reviews_count": count,
                "avg_rating": round(avg, 4) if avg is not None else None,
                "low_rating_pct": round(low_pct, 2) if low_pct is not None else None,
                "first_review_date": s["first_review_date"].date().isoformat()
                if s["first_review_date"]
                else None,
                "last_review_date": s["last_review_date"].date().isoformat()
                if s["last_review_date"]
                else None,
            }
        )

    out_rows.sort(key=lambda x: (x["app_name"] or "", x["app_id"] or ""))
    return out_rows


def compute_daily_kpis(rows):
    stats = defaultdict(lambda: {"reviews_count": 0, "score_sum": 0.0})

    for r in rows:
        at = parse_ts(r.get("at"))
        score = r.get("score")
        if not at:
            continue
        day = at.date().isoformat()
        stats[day]["reviews_count"] += 1
        try:
            stats[day]["score_sum"] += int(score)
        except Exception:
            pass

    out_rows = []
    for day, s in stats.items():
        count = s["reviews_count"]
        avg = s["score_sum"] / count if count else None
        out_rows.append(
            {
                "date": day,
                "daily_reviews": count,
                "daily_avg_rating": round(avg, 4) if avg is not None else None,
            }
        )

    out_rows.sort(key=lambda x: x["date"])
    return out_rows


def write_csv(path, rows, fieldnames):
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def main():
    rows = list(load_reviews())
    app_kpis = compute_app_kpis(rows)
    daily_kpis = compute_daily_kpis(rows)

    write_csv(
        APP_KPI_OUT,
        app_kpis,
        [
            "app_id",
            "app_name",
            "reviews_count",
            "avg_rating",
            "low_rating_pct",
            "first_review_date",
            "last_review_date",
        ],
    )
    write_csv(
        DAILY_KPI_OUT,
        daily_kpis,
        ["date", "daily_reviews", "daily_avg_rating"],
    )

    print(f"Wrote {APP_KPI_OUT}")
    print(f"Wrote {DAILY_KPI_OUT}")


if __name__ == "__main__":
    main()
