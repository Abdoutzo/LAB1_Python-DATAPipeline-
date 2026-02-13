import csv
import json
import os
import subprocess
import sys
from datetime import datetime
from pathlib import Path


ROOT_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT_DIR / "data" / "raw"
PROCESSED_DIR = ROOT_DIR / "data" / "processed"

APPS_OUT = PROCESSED_DIR / "apps.csv"
REVIEWS_OUT = PROCESSED_DIR / "reviews.csv"
APP_KPI_OUT = PROCESSED_DIR / "app_kpis.csv"
DAILY_KPI_OUT = PROCESSED_DIR / "daily_kpis.csv"
SENTIMENT_KPI_OUT = PROCESSED_DIR / "sentiment_mismatch_kpis.csv"
QUALITY_REPORT_OUT = PROCESSED_DIR / "quality_report.json"
REPORT_OUT = PROCESSED_DIR / "stress_test_report.md"

DEFAULT_APPS = RAW_DIR / "apps.jsonl"
DEFAULT_REVIEWS = [RAW_DIR / "reviews.jsonl"]

SCENARIOS = [
    {
        "id": "new_reviews_batch",
        "title": "1) New Reviews Batch",
        "apps_path": DEFAULT_APPS,
        "reviews_paths": [RAW_DIR / "note_taking_ai_reviews_batch2.csv"],
        "required_files": [RAW_DIR / "note_taking_ai_reviews_batch2.csv"],
    },
    {
        "id": "schema_drift_reviews",
        "title": "2) Schema Drift in Reviews",
        "apps_path": DEFAULT_APPS,
        "reviews_paths": [RAW_DIR / "note_taking_ai_reviews_schema_drift.csv"],
        "required_files": [RAW_DIR / "note_taking_ai_reviews_schema_drift.csv"],
    },
    {
        "id": "dirty_reviews",
        "title": "3) Dirty and Inconsistent Data Records",
        "apps_path": DEFAULT_APPS,
        "reviews_paths": [RAW_DIR / "note_taking_ai_reviews_dirty.csv"],
        "required_files": [RAW_DIR / "note_taking_ai_reviews_dirty.csv"],
    },
    {
        "id": "updated_apps_metadata",
        "title": "4) Updated Applications Metadata",
        "apps_path": RAW_DIR / "note_taking_ai_apps_updated.csv",
        "reviews_paths": DEFAULT_REVIEWS,
        "required_files": [RAW_DIR / "note_taking_ai_apps_updated.csv"],
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


def build_markdown(results):
    now = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
    lines = []
    lines.append("# Part C Stress Test Report")
    lines.append("")
    lines.append(f"Generated at: {now}")
    lines.append("")
    lines.append("## Scenario Summary")
    lines.append("")
    lines.append(
        "| Scenario | Status | Missing files | Duplicate reviews | Unknown apps in reviews | Invalid scores | Invalid timestamps |"
    )
    lines.append(
        "| --- | --- | --- | ---: | ---: | ---: | ---: |"
    )

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
    lines.append("## Details")
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
                "  - apps duplicates: {duplicate_app_ids}, missing app_id: {missing_app_id}".format(
                    **apps_q
                )
            )
            lines.append(
                "  - reviews duplicates: {duplicate_review_ids}, unknown app_id: {unknown_app_id}, invalid score: {invalid_score}, invalid timestamp: {invalid_timestamp}".format(
                    **reviews_q
                )
            )
        lines.append("")

    lines.append("## Notes")
    lines.append("")
    lines.append("- The pipeline runs as full refresh in each scenario.")
    lines.append("- No raw file is modified during this process.")
    lines.append(
        "- At the end of stress execution, baseline outputs are restored using default raw files."
    )
    lines.append("")
    return "\n".join(lines)


def restore_baseline():
    baseline = run_pipeline(DEFAULT_APPS, DEFAULT_REVIEWS)
    if not baseline["success"]:
        print("Warning: failed to restore baseline outputs.")
        print((baseline["stderr"] or baseline["stdout"]).strip())


def main():
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    results = [run_scenario(scenario) for scenario in SCENARIOS]
    REPORT_OUT.write_text(build_markdown(results), encoding="utf-8")
    print(f"Wrote {REPORT_OUT}")
    restore_baseline()


if __name__ == "__main__":
    main()
