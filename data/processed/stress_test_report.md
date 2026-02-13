# Part C Stress Test Report

Generated at: 2026-02-13 16:28:34 UTC

## Scenario Summary

| Scenario | Status | Missing files | Duplicate reviews | Unknown apps in reviews | Invalid scores | Invalid timestamps |
| --- | --- | --- | ---: | ---: | ---: | ---: |
| 1) New Reviews Batch | ok | - | 5 | 5 | 0 | 0 |
| 2) Schema Drift in Reviews | ok | - | 0 | 0 | 0 | 0 |
| 3) Dirty and Inconsistent Data Records | ok | - | 0 | 0 | 23 | 17 |
| 4) Updated Applications Metadata | ok | - | 0 | 1187 | 0 | 0 |

## Details

### 1) New Reviews Batch

- Status: `ok`
- Output row counts:
  - apps: 30, reviews: 120, app_kpis: 6, daily_kpis: 3, sentiment_kpis: 6
- Quality counters:
  - apps duplicates: 0, missing app_id: 0
  - reviews duplicates: 5, unknown app_id: 5, invalid score: 0, invalid timestamp: 0

### 2) Schema Drift in Reviews

- Status: `ok`
- Output row counts:
  - apps: 30, reviews: 140, app_kpis: 1, daily_kpis: 5, sentiment_kpis: 1
- Quality counters:
  - apps duplicates: 0, missing app_id: 0
  - reviews duplicates: 0, unknown app_id: 0, invalid score: 0, invalid timestamp: 0

### 3) Dirty and Inconsistent Data Records

- Status: `ok`
- Output row counts:
  - apps: 30, reviews: 150, app_kpis: 2, daily_kpis: 5, sentiment_kpis: 2
- Quality counters:
  - apps duplicates: 0, missing app_id: 0
  - reviews duplicates: 0, unknown app_id: 0, invalid score: 23, invalid timestamp: 17

### 4) Updated Applications Metadata

- Status: `ok`
- Output row counts:
  - apps: 31, reviews: 18898, app_kpis: 30, daily_kpis: 736, sentiment_kpis: 30
- Quality counters:
  - apps duplicates: 4, missing app_id: 1
  - reviews duplicates: 0, unknown app_id: 1187, invalid score: 0, invalid timestamp: 0

## Notes

- The pipeline runs as full refresh in each scenario.
- No raw file is modified during this process.
- At the end of stress execution, baseline outputs are restored using default raw files.
