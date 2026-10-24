# AI-Powered Log Anomaly Detection System — Final Project Handoff

**Document status:** Written from the current, verified local implementation.  
**Sources of truth:** `results/metrics.json`, `src/anomaly_pipeline.py`, `src/train_model.py`, `app/main.py`, `dashboard/index.html`, `README.md`, `data/HDFS/*`, `results/anomaly_results.csv`, and `artifacts/isolation_forest_pipeline.joblib`.  
**Scope:** Local FastAPI demo on HDFS_2k. This is not a production deployment and does not claim cloud, CI, or multi-host rollout.

Measured results and proposed future work are kept in separate sections.

---

## 1. Project overview, problem statement and objectives

### Overview

This project is a local end-to-end demo that:

1. Loads 2,000 structured HDFS log lines.
2. Joins each line to a block-level Normal/Anomaly label through `BlockId`.
3. Trains an Isolation Forest detector with a leakage-safe BlockId split.
4. Saves the model, scored corpus, and evaluation report.
5. Serves a FastAPI backend and a browser dashboard.

### Problem statement

Operations teams need to flag unusual HDFS log activity. The provided labels are **block-level** (`anomaly_label.csv`: one Normal/Anomaly label per `BlockId`). The working sample is **HDFS_2k** (2,000 parsed lines). The original trainer scored every line after fitting on the full file, leaked frequency features, and mixed that in-sample score with academic metrics.

The scientific problem is therefore two-fold:

- Detect unusual log/block patterns with an unsupervised Isolation Forest.
- Evaluate those detections against HDFS block labels **without** training on the test blocks or tuning the threshold on the test set.

### Objectives (what was actually built)

- Verify dataset integrity (headers, BlockId extraction, label join).
- Reproduce the original in-sample baseline.
- Improve feature engineering and Isolation Forest settings using **validation only**.
- Report official metrics on **held-out test blocks**.
- Keep `/api/health`, `/api/stats`, `/api/anomalies`, and `/api/retrain` working.
- Present corpus dashboard counts separately from held-out evaluation.

Isolation Forest is not a supervised classifier. Labels are used only to exclude known anomalies from training when `train_on=normal`, to pick a validation threshold, and to compute metrics.

---

## 2. Actual modular architecture and data flow

```
data/HDFS/HDFS_2k.log_structured.csv
data/HDFS/anomaly_label.csv
        │
        ▼
src/anomaly_pipeline.py
  load_raw → BlockId extract → label merge
  add_line_features → lines_to_blocks
  split_block_ids (60/20/20, stratified)
  frequency_maps(train only)
  Isolation Forest (train) + threshold (validation)
  score test blocks + map scores back to lines
        │
        ├── artifacts/isolation_forest_pipeline.joblib
        ├── results/anomaly_results.csv
        └── results/metrics.json
        │
        ▼
app/main.py  (FastAPI)
  GET  /                 → dashboard/index.html
  GET  /api/health
  GET  /api/stats        → corpus counts from anomaly_results.csv
  GET  /api/anomalies    → top-scoring lines from the CSV
  POST /api/retrain      → train_and_evaluate()
  GET  /api/hdfs-full-metrics → read-only results/hdfs_full_metrics.json
        │
        ▼
dashboard/index.html
```

Supporting files that exist but are not on the live scoring path:

- `src/train_model.py` — CLI entry that calls `train_and_evaluate()` and prints the report.
- `src/inspect_data.py` — read-only dataset inspector. Not used by the API.
- `data/HDFS/HDFS_2k.log` — raw 2,000-line log. Training uses the structured CSV.
- `data/HDFS/HDFS_2k.log_templates.csv` — 14 templates matching the structured `EventId`s.
- `data/HDFS/HDFS_templates.csv` — 30-template Loghub-style list. **Not used for training** (different EventId numbering).
- `models/` and `notebooks/` — empty placeholders (`.gitkeep` only).

Live dashboard reads `results/anomaly_results.csv`. The Joblib file is written by training and is loadable through `load_bundle()` / `load_model()`. The API does not call `load_model()` for scoring.

---

## 3. Isolation Forest, preprocessing, features, training and evaluation

### Algorithm

scikit-learn `IsolationForest` inside a `Pipeline`:

- `ColumnTransformer`: `OneHotEncoder(handle_unknown="ignore")` on categoricals, `StandardScaler` on numerics.
- Isolation Forest: `n_estimators=200`, `max_samples=auto`, `contamination=auto`, `random_state=42`, `n_jobs=-1` (selected settings).
- Anomaly score = `-decision_function(X)` (higher = more anomalous).
- A line/block is flagged if `score >= decision_threshold`.
- The selected threshold is `0.18237117442894935`, chosen on **validation** by maximizing F1, then precision.

A small grid is searched on train/validation only:

| Axis | Values |
|---|---|
| `train_on` | `normal`, `all` |
| `n_estimators` | 200, 400 |
| `max_samples` | `auto`, 256 |
| `contamination` | `auto` (threshold is tuned separately) |

Validation selected `train_on=normal` (fit on normal train blocks only), `n_estimators=200`, `max_samples=auto`.

### Preprocessing

- Read `Date`, `Time`, `Pid` as strings so leading zeros in `Date` (`081109`) are kept.
- Extract the **first** `blk_-?\d+` from `Content`.
- Left-join `anomaly_label.csv` on `BlockId`. Unmatched labels become `Unknown` (none in HDFS_2k).
- Line features are lexical/semantic only (no labels).
- Blocks are aggregated with mean/max/mode and `n_lines` / `n_events`.

### Feature engineering

**Categorical:** `Level`, `Component`, `EventId`.

**Numeric (improved model):** `message_length`, `token_count`, `digit_count`, `ip_count`, `has_exception`, `is_delete`, `is_replicate`, `is_verify`, `is_served`, `is_warn`, `block_mentions`, `hour`, `n_lines`, `n_events`, `event_frequency`, `component_frequency`, `event_component_frequency`.

Frequency maps are fit on **train blocks only**, then applied to validation, test, and the full corpus.

**Baseline feature set** (used only for comparison): original numeric list with `has_warning_word` instead of the extra semantic flags / block counts, Isolation Forest `contamination=0.05`, default `-1` predictions.

### Training / evaluation methodology

1. Build line features; aggregate to one row per `BlockId`.
2. Keep blocks labeled Anomaly or Normal (all 1,994 HDFS_2k blocks).
3. Stratified BlockId split, `random_state=42`: 60% train / 20% validation / 20% test.
4. Fit frequency maps on train.
5. Reproduce the original in-sample baseline (fit+score all 2,000 lines).
6. Reproduce a fair baseline (original features, fit train, score test blocks).
7. Search improved Isolation Forest on train; tune threshold on validation.
8. Official metrics: held-out **test blocks**. Line-level test metrics are also stored.
9. Score every block with the chosen model+threshold and write line-level `anomaly_results.csv` for the dashboard.

Test blocks are not used to pick hyperparameters or the threshold.

---

## 4. Module responsibilities

### `src/anomaly_pipeline.py`

All training, feature, split, scoring, and persistence logic. Public entry points:

- `load_and_prepare()` — load + features; **full-file** frequencies (dashboard fallback only).
- `train_and_evaluate()` — leakage-safe train/eval; writes Joblib, CSV, and `metrics.json`.
- `load_model()` / `load_bundle()` — load the saved pipeline (and threshold/maps when present).

### `src/train_model.py`

CLI: `python src/train_model.py`. Calls `train_and_evaluate()`, prints baseline / validation / test metrics, settings, limitations, and the top 10 scored lines.

### `app/main.py`

FastAPI app (`title="AI Log Anomaly Detection API"`, `version="1.0"`). CORS is open (`allow_origins=["*"]`). `/` returns `dashboard/index.html`. `/static` mounts the dashboard folder. `result_df()` reads `results/anomaly_results.csv`, trains if the file is missing, and falls back to `load_and_prepare()` on exception (that fallback has no `anomaly_prediction`).

### `dashboard/index.html`

Nexora Observability SPA with mock login. Loads Chart.js from `/static/chart.umd.min.js`. Calls `/api/health`, `/api/stats`, and `/api/anomalies?limit=20` on load and Refresh. Retrain POSTs `/api/retrain` and stores the held-out payload in `sessionStorage`. KPI cards are corpus-only.

### `src/inspect_data.py`

Optional inspector. Lists `data/HDFS` files and prints CSV/log summaries. Expects a fuller HDFS bundle (`Event_traces.csv`, `Event_occurrence_matrix.csv`, `HDFS.npz`) that is **not** in this project.

---

## 5. API endpoints and actual behaviour

| Method | Path | Behaviour |
|---|---|---|
| GET | `/` | Returns `dashboard/index.html` as HTML. |
| GET | `/static/...` | Static files from `dashboard/`. |
| GET | `/api/health` | `{"status": "ok", "service": "AI Log Anomaly Detection"}`. Does not check the model file. |
| GET | `/api/stats` | Corpus stats from `anomaly_results.csv`: `total_logs`, `detected_anomalies` (sum of `anomaly_prediction`), `actual_anomalies` (Label == Anomaly), `anomaly_rate` (detected/total × 100, 2 d.p.), `event_types`, `components`. |
| GET | `/api/anomalies` | Query `limit` (1–200, default 20). Highest `anomaly_score` rows with `LineId`, `BlockId`, `EventId`, `Level`, `Component`, `Content`, `anomaly_score` (4 d.p.), `Label`. |
| POST | `/api/retrain` | Runs `train_and_evaluate()` (about 13–20 seconds here) and returns a mixed payload (below). |
| GET | `/api/hdfs-full-metrics` | Read-only measured HDFS_v1 results from `results/hdfs_full_metrics.json`. Does not retrain or touch the 2k CSV. |

### `/api/retrain` payload (do not mix these fields)

**Corpus counts**

- `total_logs`, `actual_anomalies`, `detected_anomalies`

**Held-out test-block metrics (official)**

- `precision`, `recall`, `f1`, `false_positive_rate`, `tp`, `tn`, `fp`, `fn`
- `evaluation`: `"held_out_test_blocks"`
- `test_blocks`, `test_actual_anomalies`, `test_detected_anomalies`

**Settings / comparison**

- `processing_time_sec`, `baseline_in_sample_f1`, `baseline_held_out_f1`, `improved_held_out_f1`
- `decision_threshold`, `train_on`, `n_estimators`, `max_samples`

Verified live values after the last train: corpus `2000` / `69` / `7`; held-out F1 `0.1333`; threshold `0.18237117442894935`.

---

## 6. Dataset, split, model settings and artifacts

### Dataset files present

| File | Role |
|---|---|
| `data/HDFS/HDFS_2k.log` | 2,000 raw lines |
| `data/HDFS/HDFS_2k.log_structured.csv` | 2,000 parsed rows: LineId, Date, Time, Pid, Level, Component, Content, EventId, EventTemplate |
| `data/HDFS/HDFS_2k.log_templates.csv` | 14 EventId templates used by the structured file |
| `data/HDFS/HDFS_templates.csv` | 30 templates; unused in training |
| `data/HDFS/anomaly_label.csv` | 575,061 BlockId labels: 558,223 Normal, 16,838 Anomaly (2.93%) |

Verified on this sample:

- Every structured line has at least one `BlockId`.
- 1,994 unique BlockIds; **all** match the label file.
- 1,931 Normal lines / 69 Anomaly lines; 1,926 Normal blocks / 68 Anomaly blocks.
- Almost one line per block (mean ≈ 1.003). That is why sequence models cannot be trained here.
- 263 two-BlockId lines repeat the same id. Three bulk-delete lines mention many blocks; only the first id is used.

Missing relative to `inspect_data.py`’s “full HDFS bundle” list: `Event_traces.csv`, `Event_occurrence_matrix.csv`, `HDFS.npz`.

### Split (from `metrics.json`)

| Split | Blocks | Anomalous blocks | Fraction |
|---|---|---|---|
| Train | 1,196 | 41 | 0.60 |
| Validation | 399 | 14 | 0.20 |
| Test | 399 | 13 | 0.20 |

Unit = `BlockId`. Stratified by block Label. `random_state=42`. No BlockId overlap.

### Selected model settings

- Isolation Forest, `train_on=normal`
- `n_estimators=200`, `max_samples=auto`, `contamination=auto`, `random_state=42`
- Threshold `0.18237117442894935` on `-decision_function`
- Frequency maps: train blocks only

### Saved artifacts

| Path | Contents |
|---|---|
| `artifacts/isolation_forest_pipeline.joblib` | Dict: `pipeline`, `threshold`, `features`, `categorical`, `numeric`, `frequency_maps`, `settings` |
| `results/anomaly_results.csv` | 2,000 scored lines, including `split`, `anomaly_prediction`, `anomaly_score` |
| `results/metrics.json` | Full baseline / validation / test report |

Current corpus file: 2,000 rows, 7 lines with `anomaly_prediction=1`.

---

## 7. Measured metrics

All figures below are from `results/metrics.json` after the last `train_and_evaluate()` run. They are measured, not invented. `processing_time_sec` changes on every retrain.

### Baseline A — original protocol (in-sample, all lines)

Fit and score all 2,000 lines. Original features. `contamination=0.05`.

| Metric | Value |
|---|---|
| n | 2,000 |
| Actual anomalies | 69 |
| Detected | 99 |
| Precision | 0.0303 |
| Recall | 0.0435 |
| F1 | **0.0357** |
| FPR | 0.0497 |
| TP / TN / FP / FN | 3 / 1,835 / 96 / 66 |
| Time (this run) | 0.27 s |

This number is **not** comparable to a held-out test score.

### Baseline B — original features, held-out test blocks

Same original features and `contamination=0.05`, but fit on train and scored on 399 test blocks.

| Metric | Value |
|---|---|
| n | 399 |
| Actual anomalies | 13 |
| Detected | 18 |
| Precision | 0.0556 |
| Recall | 0.0769 |
| F1 | **0.0645** |
| FPR | 0.0440 |
| TP / TN / FP / FN | 1 / 369 / 17 / 12 |

### Improved — validation blocks (selection only)

Used to pick the grid winner and threshold. Not the official result.

| Metric | Value |
|---|---|
| n | 399 |
| Actual anomalies | 14 |
| Detected | 3 |
| Precision | 0.3333 |
| Recall | 0.0714 |
| F1 | 0.1176 |
| FPR | 0.0052 |
| TP / TN / FP / FN | 1 / 383 / 2 / 13 |
| Threshold | 0.18237117442894935 |

### Improved — held-out test blocks (official)

| Metric | Value |
|---|---|
| n | 399 |
| Actual anomalies | 13 |
| Detected | 2 |
| Precision | **0.5000** |
| Recall | **0.0769** |
| F1 | **0.1333** |
| FPR | **0.0026** |
| TP / TN / FP / FN | 1 / 385 / 1 / 12 |

Held-out **line** metrics on this sample are identical (399 lines, same confusion matrix) because almost every block has one line.

### Corpus view (dashboard / `/api/stats`, not official F1)

| Metric | Value |
|---|---|
| Total log lines | 2,000 |
| Ground-truth anomalous lines | 69 |
| Model-flagged lines | 7 |
| Corpus anomaly rate | 0.35% |

The seven flagged lines are high-score `E14` “Verification succeeded” events. That is what Isolation Forest isolated on this sample.

### How to read the comparison

F1 rose from 0.0357 (in-sample baseline) / 0.0645 (fair baseline) to **0.1333** on the same test blocks, mainly from higher precision and lower FPR. Recall stayed **1 of 13** test anomalies (~7.7%). This is a modest, honest gain — not a strong detector.

Last recorded full `train_and_evaluate()` wall time is in `metrics.json` `processing_time_sec` (varies by machine; observed about 10–60 s, including baseline + grid search).

---

## 8. Dashboard functionality and screenshots to capture

Implemented in `dashboard/index.html` + `app.css` + `app.js` (Nexora Observability demo shell, verified against the running API):

- Dark navy enterprise shell, mock sign-in, LIVE badge tied to `/api/health` (not a log stream).
- Pages: Overview, Live Logs, Alerts & Detection Center, Anomaly Investigation, Model Evaluation, Activity History.
- **Refresh** and **Retrain**, with wait overlay (no progress-stream endpoint), banners, and a flagged-count toast.
- Four live KPI cards from `/api/stats` (corpus). Alerts totals use `detected_anomalies`, not `Label`.
- Held-out evaluation panel, filled only after a successful `/api/retrain` in that browser session (`sessionStorage` key `hdfsEvalMetrics`).
- Corpus stacked bar: detected vs remaining lines.
- EventId and Component bars from the 20 `/api/anomalies` rows (not a full-corpus histogram). On the current scores those bars are E14 and `dfs.DataBlockScanner`.
- Table of the eight `/api/anomalies` fields, with escaped cell text.
- Charts use local `dashboard/chart.umd.min.js`.

There are **no screenshot files in this repository**. For a viva or slide deck, capture:

1. Header: health pill, last-refreshed time, both buttons.
2. Four corpus KPI cards (2000 / 7 / 0.35% / 14) with the “corpus, not test F1” subtitle visible.
3. Empty evaluation panel **before** retrain.
4. Evaluation panel **after** retrain (0.5000 / 0.0769 / 0.1333 / 0.0026).
5. Corpus bar + EventId/Component charts.
6. Highest-scoring table (top row LineId 569, E14, score 0.1883, Label Normal — as last verified).
7. Browser at ~768 px width (two-column KPI wrap).

Do not screenshot `/api/stats` counts and label them as test F1.

---

## 9. Limitations (measured) and future work (proposed)

### Measured limitations

- **Low recall.** Official test recall is 0.0769 (1 TP, 12 FN). The model mostly flags rare `E14` lines, not common-template failures (`E6`, `E10`, `E11`, `E13`, …).
- **HDFS_2k has almost no sequences.** Mean lines per block ≈ 1. Standard HDFS detectors use event sequences / count vectors per block. That signal is not in this file.
- **Small evaluation set.** 13 test anomalies / 14 validation anomalies. One extra TP or FP moves F1 a lot.
- **First BlockId only** on multi-block delete lines.
- **Dashboard ≠ academic score.** KPI cards are corpus flags (7 / 2000). Official metrics are 399 test blocks.
- **`/api/retrain` mixes scopes** in one JSON object (documented above).
- If the results CSV is missing or unreadable, `load_and_prepare()` has no `anomaly_prediction` and `/api/stats` can fail. Keep a valid CSV for the demo.
- **Google Fonts** still load from the network; the UI falls back to Segoe UI / system fonts.
- **No automated test suite, Docker image, or remote deployment** is in this repo.

### Proposed future work (not implemented)

- Use the full HDFS traces (`Event_traces.csv` / session sequences), not HDFS_2k alone.
- Sequence or count-vector models (DeepLog-style, Drain + event histograms). These are **not** in the current code.
- A dedicated evaluation endpoint that does not mix corpus counts with test confusion-matrix fields.
- Persist last evaluation into a file the dashboard can read without retraining.
- Wire `load_model()` for live scoring of new lines (the dashboard currently reads the results CSV).
- A small pytest/API smoke test.

---

## 10. Windows setup, launch commands and live-demo sequence

From the `ready` folder (the folder that contains `app/`, `src/`, `data/`, `dashboard/`).

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python src/train_model.py
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

`README.md` also shows `uvicorn app.main:app --reload` after activating the venv. Open http://127.0.0.1:8000

Pinned dependencies in `requirements.txt` are unversioned package names: `pandas`, `numpy`, `scikit-learn`, `joblib`, `fastapi`, `uvicorn[standard]`.

### Live-demo sequence (about 3–4 minutes)

Follow `docs/PANEL_PRESENTATION.md` for the 8-minute panel version.

1. Show `data/HDFS/HDFS_2k.log_structured.csv` and `anomaly_label.csv`. State: 2,000 lines, block-level labels, ~1 line per block.
2. Open http://127.0.0.1:8000 → **Enter Demo Workspace**.
3. Point at **corpus** KPIs (2,000 / 7 / 0.35%). Say they are not test F1. LIVE = API health.
4. Click **Refresh**. Timestamp updates.
5. Open **Alerts & Detection Center**: 7 model-flagged vs 69 labelled.
6. Click **Retrain model**. Wait for the overlay. Model Evaluation fills with held-out P/R/F1 (0.50 / 0.077 / 0.133).
7. Contrast: corpus 7 flags vs test 1 TP / 12 FN.
8. Optional: open `results/metrics.json` and read the official block table aloud.
9. Close by stating the limitation: Isolation Forest cannot recover HDFS sequence anomalies from single-line blocks.

---

## Suggested final presentation outline

1. **Problem (2 min).** HDFS block labels vs log lines; why in-sample scoring is invalid.
2. **Data (2 min).** HDFS_2k, 1,994 blocks, 68 anomalies, missing sequences.
3. **Method (3 min).** Isolation Forest, train-only frequencies, BlockId 60/20/20, validation threshold.
4. **Results (4 min).** Baseline 0.036 / 0.065 vs official test F1 0.133; recall still 7.7%; confusion matrices.
5. **System (3 min).** Live dashboard: corpus KPIs, retrain, held-out panel.
6. **Limitations and future work (2 min).** Data, not “the model failed to train.”
7. **Q&A.**

Suggested slides: architecture diagram (section 2), split table, four-row metrics table (section 7), one dashboard screenshot pair (corpus vs evaluation).

---

## Suggested technical viva questions

1. Why are HDFS labels evaluated at **block** level rather than line level?
2. Why is fitting Isolation Forest on all 2,000 lines and then scoring those same lines not an official result?
3. What leaks if event frequencies are computed on the full file?
4. How is the decision threshold chosen, and why must that happen on validation rather than test?
5. Isolation Forest does not use a label as `y`. How are labels used in this project?
6. What does `train_on=normal` change compared with fitting on all train blocks?
7. Why is recall still about 8% after feature work and threshold tuning?
8. Why can DeepLog-style sequence models not be demonstrated on this HDFS_2k file?
9. `/api/retrain` returns both `detected_anomalies=7` and `f1=0.133`. What does each number describe?
10. What does `/api/stats` `anomaly_rate` measure, and why is it not F1?
11. How is `BlockId` extracted, and what happens on a delete line that lists many blocks?
12. Why is `HDFS_templates.csv` unused?
13. What is in the Joblib bundle besides the sklearn pipeline?
14. If `results/anomaly_results.csv` is deleted, what does the API do?
15. Charts are bundled locally. What still needs network (Google Fonts), and what happens if fonts fail?

---

## Source files inspected

| Path | Used for |
|---|---|
| `src/anomaly_pipeline.py` | Algorithm, split, features, persistence, API metric keys |
| `src/train_model.py` | CLI behaviour |
| `src/inspect_data.py` | Expected-but-missing HDFS files |
| `app/main.py` | Endpoints and `result_df()` |
| `dashboard/index.html` | UI behaviour |
| `README.md` | Setup commands and documented protocol |
| `requirements.txt` | Dependencies |
| `results/metrics.json` | Official numbers |
| `results/anomaly_results.csv` | Corpus 2,000 / 7 flags |
| `artifacts/isolation_forest_pipeline.joblib` | Confirmed present (bundle written by training) |
| `data/HDFS/HDFS_2k.log` | Raw line count |
| `data/HDFS/HDFS_2k.log_structured.csv` | Structured schema |
| `data/HDFS/HDFS_2k.log_templates.csv` | 14 templates |
| `data/HDFS/HDFS_templates.csv` | Unused 30-template file |
| `data/HDFS/anomaly_label.csv` | 575,061 labels |

Also confirmed during earlier verification work (not re-run for this document’s every cell): BlockId join coverage, no split overlap, frequency maps equal to a train-only recompute, and saved-model scores matching `metrics.json`.

---

## Information that could not be verified from the repo

- **No screenshot assets** exist under `docs/` or `dashboard/`. Capture them live.
- **No published paper / citation list** is in the repo. Do not invent Loghub bibliography entries unless you add them from an external source yourself.
- **No Docker, systemd, nginx, or cloud deploy config.**
- **No pinned dependency versions** in `requirements.txt` (only package names). The environment used during development was Python 3.12 with whatever pip resolved at install time.
- **Exact wall-clock time** varies by machine. Use `metrics.json` `processing_time_sec` for the last recorded full train (includes baseline + grid search).
- **`models/` and `notebooks/`** have no project content.
- **Full HDFS session traces** as Drain event sequences are not in the 2k demo. A later streaming evaluation on `data/HDFS_full` is documented in `docs/HDFS_FULL_EVALUATION.md` and `results/hdfs_full_metrics.json`; it does not change the 2k API or Joblib bundle.
- **Google Fonts** still need network; system fonts are the fallback.
- **`load_model()` live scoring** is not used by the API; only the CSV path is used.

If a slide or report needs a number that is not in `results/metrics.json` or this handoff, recompute it from the saved model rather than guessing.
