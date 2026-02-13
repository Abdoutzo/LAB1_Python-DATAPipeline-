import pandas as pd
from pathlib import Path
import re


PROCESSED_DIR = Path("data/processed")
REVIEWS_IN = PROCESSED_DIR / "reviews.csv"

APP_KPI_OUT = PROCESSED_DIR / "app_kpis.csv"
DAILY_KPI_OUT = PROCESSED_DIR / "daily_kpis.csv"
SENTIMENT_KPI_OUT = PROCESSED_DIR / "sentiment_mismatch_kpis.csv"

POSITIVE_WORDS = {
    "good",
    "great",
    "excellent",
    "awesome",
    "amazing",
    "love",
    "helpful",
    "best",
    "fast",
    "useful",
    "perfect",
    "easy",
}

NEGATIVE_WORDS = {
    "bad",
    "poor",
    "terrible",
    "awful",
    "worst",
    "hate",
    "bug",
    "bugs",
    "slow",
    "broken",
    "crash",
    "crashes",
    "problem",
    "useless",
}


def load_reviews():
    df = pd.read_csv(REVIEWS_IN)
    required = [
        "app_id",
        "app_name",
        "reviewId",
        "userName",
        "score",
        "content",
        "thumbsUpCount",
        "at",
    ]
    for col in required:
        if col not in df.columns:
            df[col] = pd.NA

    df["score"] = pd.to_numeric(df["score"], errors="coerce")
    df["thumbsUpCount"] = (
        pd.to_numeric(df["thumbsUpCount"], errors="coerce").fillna(0).astype(int)
    )
    df["content"] = df["content"].fillna("")
    df["content_length"] = df["content"].str.strip().str.len()
    df["at_dt"] = pd.to_datetime(df["at"], errors="coerce")
    return df


def rating_std(series):
    s = series.dropna()
    if len(s) <= 1:
        return 0.0
    return float(s.std(ddof=0))


def compute_review_velocity(counts, days_active):
    velocity = counts.astype(float)
    mask_pos = days_active > 0
    velocity.loc[mask_pos] = (counts[mask_pos] / days_active[mask_pos]).astype(float)
    velocity = velocity.where(days_active.notna(), 0.0)
    return velocity


def sentiment_label(text):
    if not isinstance(text, str):
        return "neutral"
    tokens = re.findall(r"[a-z']+", text.lower())
    if not tokens:
        return "neutral"
    pos = sum(1 for token in tokens if token in POSITIVE_WORDS)
    neg = sum(1 for token in tokens if token in NEGATIVE_WORDS)
    if pos > neg:
        return "positive"
    if neg > pos:
        return "negative"
    return "neutral"


def compute_app_kpis(df):
    g = df.groupby("app_id", dropna=False)

    reviews_count = g.size().rename("reviews_count")
    app_name = g["app_name"].first().rename("app_name")

    valid_score_count = g["score"].count().rename("valid_score_count")
    avg_rating = g["score"].mean().rename("avg_rating")
    low_count = g["score"].apply(lambda s: (s <= 2).sum()).rename("low_count")
    low_rating_pct = (low_count / valid_score_count * 100).rename("low_rating_pct")

    total_thumbs_up = g["thumbsUpCount"].sum().rename("total_thumbs_up")
    avg_thumbs_up = (total_thumbs_up / reviews_count).rename("avg_thumbs_up")
    median_thumbs_up = g["thumbsUpCount"].quantile(
        0.5, interpolation="higher"
    ).rename("median_thumbs_up")

    avg_review_length = g["content_length"].mean().rename("avg_review_length")
    empty_review_pct = g["content_length"].apply(
        lambda s: (s == 0).mean() * 100
    ).rename("empty_review_pct")

    rating_std_dev = g["score"].apply(rating_std).rename("rating_std_dev")

    first_dt = g["at_dt"].min().rename("first_review_dt")
    last_dt = g["at_dt"].max().rename("last_review_dt")

    days_active = (last_dt - first_dt).dt.days.rename("days_active")
    review_velocity = compute_review_velocity(reviews_count, days_active).rename(
        "review_velocity"
    )

    app_kpis = pd.concat(
        [
            app_name,
            reviews_count,
            avg_rating,
            low_rating_pct,
            first_dt,
            last_dt,
            total_thumbs_up,
            avg_thumbs_up,
            median_thumbs_up,
            avg_review_length,
            empty_review_pct,
            rating_std_dev,
            review_velocity,
        ],
        axis=1,
    )

    app_kpis["first_review_date"] = app_kpis["first_review_dt"].dt.strftime(
        "%Y-%m-%d"
    )
    app_kpis["last_review_date"] = app_kpis["last_review_dt"].dt.strftime("%Y-%m-%d")
    app_kpis = app_kpis.drop(columns=["first_review_dt", "last_review_dt"])

    app_kpis["avg_rating"] = app_kpis["avg_rating"].round(4)
    app_kpis["low_rating_pct"] = app_kpis["low_rating_pct"].round(2)
    app_kpis["avg_thumbs_up"] = app_kpis["avg_thumbs_up"].round(2)
    app_kpis["avg_review_length"] = app_kpis["avg_review_length"].round(1)
    app_kpis["empty_review_pct"] = app_kpis["empty_review_pct"].round(2)
    app_kpis["rating_std_dev"] = app_kpis["rating_std_dev"].round(3)
    app_kpis["review_velocity"] = app_kpis["review_velocity"].round(2)

    app_kpis["total_thumbs_up"] = app_kpis["total_thumbs_up"].fillna(0).astype(int)
    app_kpis["median_thumbs_up"] = app_kpis["median_thumbs_up"].fillna(0).astype(int)

    app_kpis = app_kpis.reset_index()
    app_kpis = app_kpis.sort_values(["app_name", "app_id"], na_position="last")

    return app_kpis[
        [
            "app_id",
            "app_name",
            "reviews_count",
            "avg_rating",
            "low_rating_pct",
            "first_review_date",
            "last_review_date",
            "total_thumbs_up",
            "avg_thumbs_up",
            "median_thumbs_up",
            "avg_review_length",
            "empty_review_pct",
            "rating_std_dev",
            "review_velocity",
        ]
    ]


def compute_daily_kpis(df):
    daily = df[df["at_dt"].notna()].copy()
    if daily.empty:
        return pd.DataFrame(columns=["date", "daily_reviews", "daily_avg_rating"])

    daily["date"] = daily["at_dt"].dt.date
    g = daily.groupby("date", dropna=False)

    daily_reviews = g.size().rename("daily_reviews")
    daily_avg_rating = g["score"].mean().rename("daily_avg_rating")

    daily_kpis = pd.concat([daily_reviews, daily_avg_rating], axis=1).reset_index()
    daily_kpis["daily_avg_rating"] = daily_kpis["daily_avg_rating"].round(4)
    daily_kpis["date"] = daily_kpis["date"].astype(str)
    daily_kpis = daily_kpis.sort_values("date")

    return daily_kpis[["date", "daily_reviews", "daily_avg_rating"]]


def compute_sentiment_mismatch_kpis(df):
    reviews = df.copy()
    reviews["sentiment_label"] = reviews["content"].apply(sentiment_label)
    reviews["mismatch_flag"] = (
        ((reviews["sentiment_label"] == "negative") & (reviews["score"] >= 4))
        | ((reviews["sentiment_label"] == "positive") & (reviews["score"] <= 2))
    ).astype(int)
    reviews["has_text"] = reviews["content"].str.strip().ne("")

    scoped = reviews[reviews["score"].notna()].copy()
    if scoped.empty:
        return pd.DataFrame(
            columns=[
                "app_id",
                "app_name",
                "reviews_with_text",
                "positive_reviews",
                "negative_reviews",
                "neutral_reviews",
                "mismatch_reviews",
                "mismatch_pct",
            ]
        )

    g = scoped.groupby("app_id", dropna=False)
    app_name = g["app_name"].first().rename("app_name")
    reviews_with_text = g["has_text"].sum().rename("reviews_with_text")
    positive_reviews = g["sentiment_label"].apply(
        lambda s: (s == "positive").sum()
    ).rename("positive_reviews")
    negative_reviews = g["sentiment_label"].apply(
        lambda s: (s == "negative").sum()
    ).rename("negative_reviews")
    neutral_reviews = g["sentiment_label"].apply(
        lambda s: (s == "neutral").sum()
    ).rename("neutral_reviews")
    mismatch_reviews = g["mismatch_flag"].sum().rename("mismatch_reviews")
    mismatch_pct = (mismatch_reviews / reviews_with_text * 100).rename("mismatch_pct")

    out = pd.concat(
        [
            app_name,
            reviews_with_text,
            positive_reviews,
            negative_reviews,
            neutral_reviews,
            mismatch_reviews,
            mismatch_pct,
        ],
        axis=1,
    ).reset_index()

    out["mismatch_pct"] = out["mismatch_pct"].fillna(0).round(2)
    out = out.sort_values(["mismatch_pct", "mismatch_reviews"], ascending=[False, False])
    return out[
        [
            "app_id",
            "app_name",
            "reviews_with_text",
            "positive_reviews",
            "negative_reviews",
            "neutral_reviews",
            "mismatch_reviews",
            "mismatch_pct",
        ]
    ]


def main():
    df = load_reviews()
    app_kpis = compute_app_kpis(df)
    daily_kpis = compute_daily_kpis(df)
    sentiment_kpis = compute_sentiment_mismatch_kpis(df)

    app_kpis.to_csv(APP_KPI_OUT, index=False)
    daily_kpis.to_csv(DAILY_KPI_OUT, index=False)
    sentiment_kpis.to_csv(SENTIMENT_KPI_OUT, index=False)

    print(f"Wrote {APP_KPI_OUT}")
    print(f"Wrote {DAILY_KPI_OUT}")
    print(f"Wrote {SENTIMENT_KPI_OUT}")


if __name__ == "__main__":
    main()
