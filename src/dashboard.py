import csv
from pathlib import Path

import matplotlib.pyplot as plt


PROCESSED_DIR = Path("data/processed")
APP_KPI_IN = PROCESSED_DIR / "app_kpis.csv"
DAILY_KPI_IN = PROCESSED_DIR / "daily_kpis.csv"
OUT_IMG = PROCESSED_DIR / "dashboard.png"


def load_app_kpis():
    rows = []
    with APP_KPI_IN.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            try:
                row["reviews_count"] = int(row["reviews_count"])
            except Exception:
                row["reviews_count"] = 0
            try:
                row["avg_rating"] = float(row["avg_rating"])
            except Exception:
                row["avg_rating"] = None
            rows.append(row)
    return rows


def load_daily_kpis():
    rows = []
    with DAILY_KPI_IN.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            try:
                row["daily_reviews"] = int(row["daily_reviews"])
            except Exception:
                row["daily_reviews"] = 0
            try:
                row["daily_avg_rating"] = float(row["daily_avg_rating"])
            except Exception:
                row["daily_avg_rating"] = None
            rows.append(row)
    return rows


def main():
    app_rows = load_app_kpis()
    daily_rows = load_daily_kpis()

    # Plot 1: top apps by review volume with average rating
    app_rows.sort(key=lambda r: r["reviews_count"], reverse=True)
    top = app_rows[:10]
    names = [r["app_name"] or r["app_id"] for r in top]
    avg = [r["avg_rating"] or 0 for r in top]
    counts = [r["reviews_count"] for r in top]

    # Plot 2: daily average rating over time
    daily_rows.sort(key=lambda r: r["date"])
    dates = [r["date"] for r in daily_rows]
    daily_avg = [r["daily_avg_rating"] or 0 for r in daily_rows]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 9))
    ax1.barh(names, avg)
    ax1.set_title("Top 10 Apps by Review Volume (Avg Rating)")
    ax1.set_xlabel("Average Rating")
    ax1.set_xlim(0, 5)
    ax1.invert_yaxis()

    # Annotate with review counts
    for i, c in enumerate(counts):
        ax1.text(avg[i] + 0.05, i, f"{c} reviews", va="center", fontsize=8)

    ax2.plot(dates, daily_avg, color="#1f77b4")
    ax2.set_title("Daily Average Rating Over Time")
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Average Rating")
    ax2.set_ylim(0, 5)
    ax2.tick_params(axis="x", labelrotation=45)

    fig.tight_layout()
    fig.savefig(OUT_IMG, dpi=150)
    print(f"Wrote {OUT_IMG}")


if __name__ == "__main__":
    main()
