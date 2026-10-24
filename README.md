# AI-Powered Log Anomaly Detection System

## Demo architecture
HDFS logs → preprocessing → feature extraction → Isolation Forest → saved Joblib model → FastAPI → web dashboard.

## Quick start (Windows)
Run these commands from this folder (the directory that contains `app/`, `src/`, `dashboard/`, `results/`, and `requirements.txt`). If your zip extracted a parent folder named `Log-Anomaly-Detection_Phase3_ready`, `cd` into the inner `ready` directory first.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python src/train_model.py
uvicorn app.main:app --reload
```
Equivalent launch if `uvicorn` is not on PATH: `python -m uvicorn app.main:app --host 127.0.0.1 --port 8000`

Open http://127.0.0.1:8000

Full write-up: `docs/FINAL_PROJECT_HANDOFF.md`.  
Panel script and numbers card: `docs/PANEL_PRESENTATION.md`.

## What is implemented
- HDFS_2k structured log loading and label matching through `BlockId`
- Numerical + categorical feature engineering
- Isolation Forest anomaly detector
- Precision, recall, F1 and false-positive-rate evaluation against the provided HDFS labels
- Leakage-safe BlockId train/validation/test split with validation threshold tuning
- Joblib model persistence
- FastAPI endpoints: `/api/health`, `/api/stats`, `/api/anomalies`, `/api/retrain`, plus read-only `/api/hdfs-full-metrics`
- Nexora Observability dashboard (mock login): Overview, Live Logs, Alerts & Detection Center, Anomaly Investigation, Model Evaluation, Activity History
- Corpus KPIs from `/api/stats` kept separate from held-out P/R/F1 after Retrain

## Dataset notes
- Structured logs: `data/HDFS/HDFS_2k.log_structured.csv` (2,000 lines, 1,994 unique blocks).
- Labels: `data/HDFS/anomaly_label.csv` (575,061 blocks; 16,838 anomalies, 2.93%).
- Every 2k `BlockId` matches the label file: 1,931 Normal lines / 69 Anomaly lines (68 anomalous blocks).
- `HDFS_2k.log_templates.csv` is the template set used by the structured file. `HDFS_templates.csv` is the full Loghub template list and is not used for training because its EventId numbering is different.
- The first `blk_<id>` is used when a line mentions multiple blocks. The 263 two-id delete lines repeat the same block; three bulk-delete lines mention many blocks and only the first id is scored.

## Evaluation protocol
HDFS labels are **block-level**. The original code scored **log lines** after training on the full file. That leaked frequency features and reused the evaluation data for fitting.

The improved trainer:
1. Builds line features, then aggregates to one row per `BlockId`.
2. Splits blocks 60/20/20, stratified by block label, `random_state=42`.
3. Fits event/component frequency maps on **train blocks only**.
4. Fits Isolation Forest on train data (normal-only if that wins validation).
5. Tunes the anomaly-score threshold on validation to maximize F1, then precision.
6. Reports official metrics on held-out **test blocks**. Line-level test metrics are also stored because the dashboard is line-oriented.

`/api/retrain` still returns the original keys (`precision`, `recall`, `f1`, ...). Those values are the held-out block metrics, not in-sample scores. Full dumps are written to `results/metrics.json`.

## Current model settings
Chosen by validation F1 (values below are from `results/metrics.json` and match the saved Joblib bundle):
- Model: Isolation Forest
- `train_on=normal` (fit on normal train blocks only)
- `n_estimators=200`, `max_samples=auto`, `contamination=auto`, `random_state=42`
- Decision threshold: `0.182371` on `score = -decision_function` (selected on validation only)
- Split: 1,196 / 399 / 399 blocks (41 / 14 / 13 anomalies), stratified by block label
- Search grid: `train_on ∈ {normal, all}`, `n_estimators ∈ {200, 400}`, `max_samples ∈ {auto, 256}`
- Features: `Level`, `Component`, `EventId`, message/token/digit/IP stats, exception/delete/replicate/verify/served/warn flags, block mention count, hour, per-block line/event counts, train-only frequencies

## Reported results
These numbers were recomputed from the saved model and are not invented.

**Baseline, original protocol** (fit and score all 2,000 lines, contamination=0.05):
precision 0.0303, recall 0.0435, F1 0.0357, FPR 0.0497, TP/FP/FN = 3/96/66.

**Baseline, fair held-out** (original features, fit on train, evaluate 399 test blocks):
precision 0.0556, recall 0.0769, F1 0.0645, FPR 0.0440, TP/FP/FN = 1/17/12.

**Improved, validation blocks** (used only to pick settings/threshold):
precision 0.3333, recall 0.0714, F1 0.1176, FPR 0.0052, TP/FP/FN = 1/2/13.

**Improved, held-out test blocks (official):**
precision 0.5000, recall 0.0769, F1 0.1333, FPR 0.0026, TP/FP/FN = 1/1/12 (399 blocks, 13 anomalies).
Held-out line-level metrics on this sample are identical because almost every block has one line.

The dashboard shows **corpus** detections from `results/anomaly_results.csv`: 2,000 lines, 7 flagged (0.35%). That is not the official test F1. `/api/retrain` returns corpus counts in `total_logs` / `detected_anomalies` and held-out block scores in `precision` / `recall` / `f1` / `tp` / `tn` / `fp` / `fn`.

## Results CSV and model fallback
`/api/stats` and `/api/anomalies` read `results/anomaly_results.csv` (they do not call `load_model()`).

- If the CSV is **missing**, `train_and_evaluate()` runs, writes the CSV, Joblib bundle, and `results/metrics.json`, then the API reads the new CSV.
- If the CSV exists but is **unreadable**, `result_df()` falls back to `load_and_prepare()`. That frame has labels and features but **no** `anomaly_prediction` / `anomaly_score`, so `/api/stats` and `/api/anomalies` can fail. Keep a valid `anomaly_results.csv` for the demo.
- The saved model is `artifacts/isolation_forest_pipeline.joblib`. Retrain or `python src/train_model.py` recreates it together with the CSV.

## Important academic note
Isolation Forest itself is unsupervised. Labels are not a classification target. They are used only to:
- exclude known anomalies from training when `train_on=normal`
- select a decision threshold on validation
- compute evaluation metrics

## Limitations
- HDFS_2k has about one log line per block. Standard HDFS detectors rely on **event sequences** (missing/extra events in a block). That signal is not present here.
- Most anomalous lines use the same common templates as normal lines (`E6`, `E10`, `E11`, `E13`, ...). Isolation Forest can isolate rare templates, not those look-alike failures.
- The saved model mainly flags rare `E14` verification lines. Recall stays about 7.7% on the test blocks (1 of 13).
- Validation/test anomaly counts are small (~13–14 blocks), so F1 has high variance.
- Do not treat dashboard counts as the official academic result; use the held-out numbers in `results/metrics.json`.

## HDFS_v1 full-corpus evaluation
The 2k demo is unchanged. A separate streaming evaluator reads `../data/HDFS_full/data/*.parquet` (or `data/HDFS_full/data` if copied next to `ready/`), aggregates by `block_id`, and writes `results/hdfs_full_metrics.json`.

```powershell
python src/hdfs_full_eval.py --subset
python src/hdfs_full_eval.py
```

Official full-corpus held-out test (115,013 blocks, 3,368 anomalies), from that JSON:

- Improved (2k settings + validation threshold): precision 0.3181, recall 0.5736, F1 0.4093, FPR 0.0371, PR-AUC 0.2370, TP/FP/FN = 1932/4141/1436.
- Baseline (contamination=0.05, original features, train fit): precision 0.3215, recall 0.5582, F1 0.4080, FPR 0.0355, PR-AUC 0.4652, TP/FP/FN = 1880/3967/1488.

F1 is a tie (+0.0012). Baseline PR-AUC is higher. Do not present the 2k settings as a full-corpus improvement. Details: `docs/HDFS_FULL_EVALUATION.md`. The dashboard Model Evaluation page reads `/api/hdfs-full-metrics`.
