import csv
from datetime import datetime
from pathlib import Path

import matplotlib.pyplot as plt
import matplotlib.dates as mdates


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
            try:
                row["avg_thumbs_up"] = float(row["avg_thumbs_up"])
            except Exception:
                row["avg_thumbs_up"] = 0
            try:
                row["avg_review_length"] = float(row["avg_review_length"])
            except Exception:
                row["avg_review_length"] = 0
            try:
                row["rating_std_dev"] = float(row["rating_std_dev"])
            except Exception:
                row["rating_std_dev"] = 0
            try:
                row["review_velocity"] = float(row["review_velocity"])
            except Exception:
                row["review_velocity"] = 0
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

    # Create a 3x2 grid for 6 visualizations
    fig, axes = plt.subplots(3, 2, figsize=(16, 14))
    fig.suptitle("AI Note-Taking Apps Analytics Dashboard", fontsize=16, fontweight="bold")

    # ============ Plot 1: Top apps by review volume with avg rating ============
    ax1 = axes[0, 0]
    app_rows.sort(key=lambda r: r["reviews_count"], reverse=True)
    top = app_rows[:10]
    names = [r["app_name"] or r["app_id"] for r in top]
    avg = [r["avg_rating"] or 0 for r in top]
    counts = [r["reviews_count"] for r in top]

    ax1.barh(names, avg, color="#1f77b4")
    ax1.set_title("Top 10 Apps by Review Volume (Avg Rating)", fontweight="bold")
    ax1.set_xlabel("Average Rating")
    ax1.set_xlim(0, 5)
    ax1.invert_yaxis()
    for i, c in enumerate(counts):
        ax1.text(avg[i] + 0.05, i, f"{c} reviews", va="center", fontsize=7)

    # ============ Plot 2: Daily average rating over time ============
    ax2 = axes[0, 1]
    daily_rows.sort(key=lambda r: r["date"])
    dates = [datetime.fromisoformat(r["date"]) for r in daily_rows if r.get("date")]
    daily_avg = [r["daily_avg_rating"] or 0 for r in daily_rows]

    ax2.plot(dates, daily_avg, color="#ff7f0e", linewidth=2)
    ax2.set_title("Daily Average Rating Over Time", fontweight="bold")
    ax2.set_xlabel("Date")
    ax2.set_ylabel("Average Rating")
    ax2.set_ylim(0, 5)
    ax2.xaxis.set_major_locator(mdates.MonthLocator(interval=2))
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))
    ax2.tick_params(axis="x", labelrotation=45)
    ax2.grid(alpha=0.3)

    # ============ Plot 3: Top apps by engagement (avg thumbs up) ============
    ax3 = axes[1, 0]
    app_rows.sort(key=lambda r: r["avg_thumbs_up"], reverse=True)
    top_engagement = app_rows[:10]
    eng_names = [r["app_name"] or r["app_id"] for r in top_engagement]
    eng_thumbs = [r["avg_thumbs_up"] for r in top_engagement]

    ax3.barh(eng_names, eng_thumbs, color="#2ca02c")
    ax3.set_title("Top 10 Apps by User Engagement (Avg Thumbs Up)", fontweight="bold")
    ax3.set_xlabel("Average Thumbs Up per Review")
    ax3.invert_yaxis()

    # ============ Plot 4: Review quality - avg review length ============
    ax4 = axes[1, 1]
    app_rows.sort(key=lambda r: r["avg_review_length"], reverse=True)
    top_quality = app_rows[:10]
    qual_names = [r["app_name"] or r["app_id"] for r in top_quality]
    qual_lengths = [r["avg_review_length"] for r in top_quality]

    ax4.barh(qual_names, qual_lengths, color="#d62728")
    ax4.set_title("Top 10 Apps by Review Detail (Avg Review Length)", fontweight="bold")
    ax4.set_xlabel("Average Review Length (characters)")
    ax4.invert_yaxis()

    # ============ Plot 5: Rating volatility (std dev) ============
    ax5 = axes[2, 0]
    app_rows.sort(key=lambda r: r["rating_std_dev"], reverse=True)
    top_volatile = app_rows[:10]
    vol_names = [r["app_name"] or r["app_id"] for r in top_volatile]
    vol_std = [r["rating_std_dev"] for r in top_volatile]

    ax5.barh(vol_names, vol_std, color="#9467bd")
    ax5.set_title("Most Polarizing Apps (Rating Std Dev)", fontweight="bold")
    ax5.set_xlabel("Rating Standard Deviation")
    ax5.invert_yaxis()

    # ============ Plot 6: Review velocity (most active apps) ============
    ax6 = axes[2, 1]
    app_rows.sort(key=lambda r: r["review_velocity"], reverse=True)
    top_velocity = app_rows[:10]
    vel_names = [r["app_name"] or r["app_id"] for r in top_velocity]
    vel_values = [r["review_velocity"] for r in top_velocity]

    ax6.barh(vel_names, vel_values, color="#8c564b")
    ax6.set_title("Most Active Apps (Review Velocity)", fontweight="bold")
    ax6.set_xlabel("Reviews per Day")
    ax6.invert_yaxis()

    # Save the dashboard
    fig.tight_layout()
    fig.savefig(OUT_IMG, dpi=150, bbox_inches="tight")
    print(f"Wrote enhanced dashboard to {OUT_IMG}")


if __name__ == "__main__":
    main()
