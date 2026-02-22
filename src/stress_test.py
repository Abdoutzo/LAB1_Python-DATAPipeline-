import csv
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / "data" / "raw"
RESOURCES_DIR = ROOT_DIR / "resources" / "session1_2_extracted"
RESOURCES_ZIP = ROOT_DIR / "Data_Engineering_-_S1-2_-_Resources.zip"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"

APPS_OUT = PROCESSED_DIR / "apps.csv"
REVIEWS_OUT = PROCESSED_DIR / "reviews.csv"
APP_KPI_OUT = PROCESSED_DIR / "app_kpis.csv"
DAILY_KPI_OUT = PROCESSED_DIR / "daily_kpis.csv"
SENTIMENT_KPI_OUT = PROCESSED_DIR / "sentiment_mismatch_kpis.csv"
QUALITY_REPORT_OUT = PROCESSED_DIR / "quality_report.json"
REPORT_OUT = PROCESSED_DIR / "stress_test_report.md"

BASE_APPS = RAW_DIR / "apps.jsonl"
BASE_REVIEWS = [RAW_DIR / "reviews.jsonl"]
REQUIRED_RESOURCE_FILES = [
    "note_taking_ai_reviews_batch2.csv",
    "note_taking_ai_reviews_schema_drift.csv",
    "note_taking_ai_reviews_dirty.csv",
    "note_taking_ai_apps_updated.csv",
]

SCENARIOS = [
    {
        "id": "new_reviews_batch",
        "title": "1) New Reviews Batch",
        "apps_path": BASE_APPS,
        "reviews_paths": [RESOURCES_DIR / "note_taking_ai_reviews_batch2.csv"],
        "required_files": [RESOURCES_DIR / "note_taking_ai_reviews_batch2.csv"],
    },
    {
        "id": "schema_drift_reviews",
        "title": "2) Schema Drift in Reviews",
        "apps_path": BASE_APPS,
        "reviews_paths": [RESOURCES_DIR / "note_taking_ai_reviews_schema_drift.csv"],
        "required_files": [RESOURCES_DIR / "note_taking_ai_reviews_schema_drift.csv"],
    },
    {
        "id": "dirty_reviews",
        "title": "3) Dirty and Inconsistent Data Records",
        "apps_path": BASE_APPS,
        "reviews_paths": [RESOURCES_DIR / "note_taking_ai_reviews_dirty.csv"],
        "required_files": [RESOURCES_DIR / "note_taking_ai_reviews_dirty.csv"],
    },
    {
        "id": "updated_apps_metadata",
        "title": "4) Updated Applications Metadata",
        "apps_path": RESOURCES_DIR / "note_taking_ai_apps_updated.csv",
        "reviews_paths": BASE_REVIEWS,
        "required_files": [RESOURCES_DIR / "note_taking_ai_apps_updated.csv"],
    },
]


def csv_rows(path):
    if not path.exists():
        return 0
    with path.open(encoding="utf-8", newline="") as f:
        reader = csv.reader(f)
        try:
            next(reader)
        except StopIteration:
            return 0
        return sum(1 for _ in reader)


def ensure_resources():
    if all((RESOURCES_DIR / name).exists() for name in REQUIRED_RESOURCE_FILES):
        return
    if not RESOURCES_ZIP.exists():
        raise FileNotFoundError(
            f"Missing stress resources zip: {RESOURCES_ZIP}. "
            f"Expected files: {', '.join(REQUIRED_RESOURCE_FILES)}"
        )
    RESOURCES_DIR.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(RESOURCES_ZIP) as zf:
        zf.extractall(RESOURCES_DIR)


def run_command(cmd, env):
    return subprocess.run(
        cmd,
        cwd=ROOT_DIR,
        env=env,
        capture_output=True,
        text=True,
    )


def run_pipeline(apps_path, reviews_paths):
    env = os.environ.copy()
    env["RAW_APPS_PATH"] = str(apps_path)
    env["RAW_REVIEWS_PATHS"] = ",".join(str(p) for p in reviews_paths)

    transform = run_command([sys.executable, "src/transform.py"], env)
    if transform.returncode != 0:
        return {
            "success": False,
            "step": "transform",
            "stdout": transform.stdout,
            "stderr": transform.stderr,
        }

    serve = run_command([sys.executable, "src/serve.py"], env)
    if serve.returncode != 0:
        return {
            "success": False,
            "step": "serve",
            "stdout": serve.stdout,
            "stderr": serve.stderr,
        }

    return {
        "success": True,
        "step": "done",
        "stdout": (transform.stdout + "\n" + serve.stdout).strip(),
        "stderr": (transform.stderr + "\n" + serve.stderr).strip(),
    }


def read_quality():
    if not QUALITY_REPORT_OUT.exists():
        return {}
    with QUALITY_REPORT_OUT.open(encoding="utf-8") as f:
        return json.load(f)


def summarize_outputs():
    return {
        "apps_rows": csv_rows(APPS_OUT),
        "reviews_rows": csv_rows(REVIEWS_OUT),
        "app_kpis_rows": csv_rows(APP_KPI_OUT),
        "daily_kpis_rows": csv_rows(DAILY_KPI_OUT),
        "sentiment_kpis_rows": csv_rows(SENTIMENT_KPI_OUT),
    }


def summarize_sentiment():
    if not SENTIMENT_KPI_OUT.exists():
        return {}
    rows = []
    with SENTIMENT_KPI_OUT.open(encoding="utf-8", newline="") as f:
        for row in csv.DictReader(f):
            try:
                row["mismatch_reviews"] = int(float(row.get("mismatch_reviews", 0) or 0))
            except ValueError:
                row["mismatch_reviews"] = 0
            try:
                row["mismatch_pct"] = float(row.get("mismatch_pct", 0) or 0)
            except ValueError:
                row["mismatch_pct"] = 0.0
            rows.append(row)
    rows.sort(key=lambda r: (r["mismatch_pct"], r["mismatch_reviews"]), reverse=True)
    non_zero = [r for r in rows if r["mismatch_reviews"] > 0]
    top = non_zero[:3]
    return {
        "apps_analyzed": len(rows),
        "apps_with_mismatch": len(non_zero),
        "total_mismatch_reviews": sum(r["mismatch_reviews"] for r in non_zero),
        "top_apps": [
            {
                "app_id": r.get("app_id"),
                "app_name": r.get("app_name"),
                "mismatch_reviews": r["mismatch_reviews"],
                "mismatch_pct": round(r["mismatch_pct"], 2),
            }
            for r in top
        ],
    }


def run_scenario(scenario):
    missing_files = [str(p) for p in scenario["required_files"] if not p.exists()]
    if missing_files:
        return {
            "id": scenario["id"],
            "title": scenario["title"],
            "status": "missing_inputs",
            "missing_files": missing_files,
            "quality": {},
            "outputs": {},
            "error_step": "",
            "error": "",
        }

    execution = run_pipeline(scenario["apps_path"], scenario["reviews_paths"])
    if not execution["success"]:
        return {
            "id": scenario["id"],
            "title": scenario["title"],
            "status": "failed",
            "missing_files": [],
            "quality": {},
            "outputs": {},
            "error_step": execution["step"],
            "error": (execution["stderr"] or execution["stdout"]).strip(),
        }

    return {
        "id": scenario["id"],
        "title": scenario["title"],
        "status": "ok",
        "missing_files": [],
        "quality": read_quality(),
        "outputs": summarize_outputs(),
        "error_step": "",
        "error": "",
    }


def run_business_logic():
    execution = run_pipeline(BASE_APPS, BASE_REVIEWS)
    if not execution["success"]:
        return {
            "id": "business_logic",
            "title": "5) New Business Logic Stress Test",
            "status": "failed",
            "error_step": execution["step"],
            "error": (execution["stderr"] or execution["stdout"]).strip(),
            "quality": {},
            "outputs": {},
            "sentiment_summary": {},
        }

    return {
        "id": "business_logic",
        "title": "5) New Business Logic Stress Test",
        "status": "ok",
        "error_step": "",
        "error": "",
        "quality": read_quality(),
        "outputs": summarize_outputs(),
        "sentiment_summary": summarize_sentiment(),
    }


def build_markdown(results, business):
    lines = []
    lines.append("# Part C Stress Test Report")
    lines.append("")
    lines.append("Input resources:")
    lines.append(f"- `resources zip`: `{RESOURCES_ZIP}`")
    lines.append(f"- `baseline apps`: `{BASE_APPS}`")
    lines.append(f"- `baseline reviews`: `{BASE_REVIEWS[0]}`")
    lines.append(f"- `stress resources`: `{RESOURCES_DIR}`")
    lines.append("")
    lines.append("## Scenario Summary (1 to 4)")
    lines.append("")
    lines.append(
        "| Scenario | Status | Missing files | Duplicate reviews | Unknown apps in reviews | Invalid scores | Invalid timestamps |"
    )
    lines.append("| --- | --- | --- | ---: | ---: | ---: | ---: |")

    for result in results:
        reviews_quality = result.get("quality", {}).get("reviews", {})
        lines.append(
            "| {title} | {status} | {missing} | {dup} | {unk} | {inv_score} | {inv_ts} |".format(
                title=result["title"],
                status=result["status"],
                missing=", ".join(result["missing_files"]) if result["missing_files"] else "-",
                dup=reviews_quality.get("duplicate_review_ids", "-"),
                unk=reviews_quality.get("unknown_app_id", "-"),
                inv_score=reviews_quality.get("invalid_score", "-"),
                inv_ts=reviews_quality.get("invalid_timestamp", "-"),
            )
        )

    lines.append("")
    lines.append("## Scenario Details")
    lines.append("")

    for result in results:
        lines.append(f"### {result['title']}")
        lines.append("")
        lines.append(f"- Status: `{result['status']}`")
        if result["missing_files"]:
            lines.append("- Missing files:")
            for path in result["missing_files"]:
                lines.append(f"  - `{path}`")
        if result["status"] == "failed":
            lines.append(f"- Failed step: `{result['error_step']}`")
            lines.append("- Error excerpt:")
            lines.append("```text")
            lines.append(result["error"][:3000])
            lines.append("```")

        outputs = result.get("outputs", {})
        if outputs:
            lines.append("- Output row counts:")
            lines.append(
                "  - apps: {apps_rows}, reviews: {reviews_rows}, app_kpis: {app_kpis_rows}, daily_kpis: {daily_kpis_rows}, sentiment_kpis: {sentiment_kpis_rows}".format(
                    **outputs
                )
            )

        quality = result.get("quality", {})
        if quality:
            apps_q = quality.get("apps", {})
            reviews_q = quality.get("reviews", {})
            lines.append("- Quality counters:")
            lines.append(
                "  - apps duplicates: {duplicate_app_ids}, missing app_id: {missing_app_id}, invalid score: {invalid_score}".format(
                    **apps_q
                )
            )
            lines.append(
                "  - reviews duplicates: {duplicate_review_ids}, unknown app_id: {unknown_app_id}, invalid score: {invalid_score}, invalid timestamp: {invalid_timestamp}".format(
                    **reviews_q
                )
            )
        lines.append("")

    lines.append("## Scenario 5 (Consumer-driven business logic)")
    lines.append("")
    lines.append(f"- Status: `{business['status']}`")
    if business["status"] == "failed":
        lines.append(f"- Failed step: `{business['error_step']}`")
        lines.append("```text")
        lines.append(business["error"][:3000])
        lines.append("```")
    else:
        outputs = business.get("outputs", {})
        if outputs:
            lines.append(
                "- Output row counts: apps={apps_rows}, reviews={reviews_rows}, app_kpis={app_kpis_rows}, daily_kpis={daily_kpis_rows}, sentiment_kpis={sentiment_kpis_rows}".format(
                    **outputs
                )
            )
        sentiment = business.get("sentiment_summary", {})
        if sentiment:
            lines.append(
                "- Sentiment mismatch summary: apps_analyzed={apps_analyzed}, apps_with_mismatch={apps_with_mismatch}, total_mismatch_reviews={total_mismatch_reviews}".format(
                    **sentiment
                )
            )
            if sentiment.get("top_apps"):
                lines.append("- Top apps with mismatch:")
                for app in sentiment["top_apps"]:
                    lines.append(
                        "  - {app_name} ({app_id}): {mismatch_reviews} mismatches ({mismatch_pct}%)".format(
                            **app
                        )
                    )
    lines.append("")
    lines.append("## Notes")
    lines.append("")
    lines.append("- Full refresh mode is used for every scenario run.")
    lines.append("- No raw file is edited by the pipeline.")
    lines.append("- Scenario inputs are taken from `resources/session1_2_extracted`.")
    lines.append("- If resources are missing, they are re-extracted from the zip.")
    lines.append("- Baseline outputs are restored at the end via default raw files.")
    lines.append("")
    return "\n".join(lines)


def restore_baseline():
    baseline = run_pipeline(BASE_APPS, BASE_REVIEWS)
    if not baseline["success"]:
        print("Warning: failed to restore baseline outputs.")
        print((baseline["stderr"] or baseline["stdout"]).strip())


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    ensure_resources()
    results = [run_scenario(scenario) for scenario in SCENARIOS]
    business = run_business_logic()
    REPORT_OUT.write_text(build_markdown(results, business), encoding="utf-8")
    print(f"Wrote {REPORT_OUT}")
    restore_baseline()


if __name__ == "__main__":
    main()
