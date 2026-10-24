# Final submission checklist

Use this with `README.md`, `docs/FINAL_PROJECT_HANDOFF.md`, and `docs/PANEL_PRESENTATION.md`. Do not treat corpus KPI counts as held-out F1.

## Application readiness

- [x] Project root is the folder that contains `app/main.py`, `src/`, `dashboard/index.html`, `results/metrics.json`, and `requirements.txt` (this `ready` directory).
- [x] One dashboard only: `dashboard/index.html` (no duplicate/outdated copy).
- [x] Isolation Forest settings and official metrics live in `results/metrics.json` and match the saved Joblib bundle.
- [x] Official held-out test-block result: P 0.5000, R 0.0769, F1 0.1333, FPR 0.0026, TP/FP/FN = 1/1/12.
- [x] Corpus dashboard view (not official F1): 2,000 lines, 7 flags, 0.35% rate.
- [x] Endpoints: `GET /api/health`, `GET /api/stats`, `GET /api/anomalies`, `POST /api/retrain`.
- [x] KPI cards are corpus (`/api/stats`). Held-out P/R/F1 appear only after Retrain (`evaluation=held_out_test_blocks`).
- [x] Alerts totals use model-flagged `detected_anomalies`, not ground-truth `Label`.
- [x] LIVE badges follow `/api/health`. They do not imply a log stream.
- [x] Chart.js is local (`dashboard/chart.umd.min.js`).

## Files to submit

Include:

- `app/`, `src/`, `dashboard/`, `data/HDFS/`, `artifacts/isolation_forest_pipeline.joblib`
- `results/metrics.json`, `results/anomaly_results.csv`
- `docs/FINAL_PROJECT_HANDOFF.md`, `docs/SUBMISSION_CHECKLIST.md`, `docs/PANEL_PRESENTATION.md`
- `README.md`, `requirements.txt`

Exclude if possible: `.venv/`, `__pycache__/`, `.cursor/`. There is no Docker/cloud config to submit.

## Screenshots to capture (none are stored in the repo)

1. Login, then header: LIVE badge + last-refreshed + Refresh / Retrain.
2. Four corpus KPI cards (2000 / 7 / 0.35% / 14) with the corpus subtitle visible.
3. Alerts & Detection Center: 7 model-flagged vs 69 labelled; sample disclaimer.
4. Model Evaluation empty, then after retrain (0.5000 / 0.0769 / 0.1333 / 0.0026).
5. Corpus bar + EventId/Component charts.
6. Anomalies table (LineId, BlockId, EventId, Level, Component, Score, Label, Content).
7. Optional: ~768 px width.

## Live-demo steps

1. `cd` to this project root. Activate `.venv`.
2. `python src/train_model.py` if artifacts are missing; otherwise start the API.
3. `uvicorn app.main:app --reload` or `python -m uvicorn app.main:app --host 127.0.0.1 --port 8000`.
4. Open http://127.0.0.1:8000 and click **Enter Demo Workspace**.
5. Show corpus KPIs; say they are not test F1. Point at LIVE = API health.
6. Open Alerts: 7 flags vs 69 labels. Review flagged logs.
7. Retrain Model (often ~10–60 s); show held-out panel on Model Evaluation.
8. Optionally open `results/metrics.json`.

## Fallback behaviour

- Missing `results/anomaly_results.csv` → API calls `train_and_evaluate()` and recreates CSV + model + metrics.
- Unreadable CSV → `load_and_prepare()` (no predictions) → `/api/stats` can fail. Keep a valid CSV for the demo.

## 15 viva questions and accurate answers

1. **Why evaluate at block level?** HDFS labels in `anomaly_label.csv` are one Normal/Anomaly per `BlockId`, not per log line.
2. **Why is fit-and-score on all 2,000 lines not official?** That reuses the evaluation rows for training (in-sample). Official F1 is on 399 held-out test blocks.
3. **What leaks if frequencies use the full file?** Test/validation event rates enter the features. Maps are fit on train blocks only.
4. **How is the threshold chosen?** Maximize validation F1, then precision, on `-decision_function`. Test is not used. Saved cutoff is 0.182371.
5. **Does Isolation Forest train on labels as y?** No. Labels only exclude known anomalies when `train_on=normal`, tune the threshold, and score metrics.
6. **What does `train_on=normal` mean?** Fit IF on normal train blocks only. Validation selected this over fitting all train blocks.
7. **Why is recall still ~8%?** Test recall is 1/13. Most anomalous lines use common templates that look like normal lines. The model mainly flags rare `E14` events.
8. **Why no DeepLog-style demo?** HDFS_2k has ~1 line per block, so event sequences are not present.
9. **`detected_anomalies=7` vs `f1=0.133`?** 7 is corpus flags on 2,000 lines. F1 is held-out test blocks (1 TP, 1 FP, 12 FN).
10. **What is `/api/stats` `anomaly_rate`?** `detected / total_logs * 100` on the corpus CSV (0.35%). It is not F1.
11. **How is BlockId extracted?** First `blk_-?\d+` in `Content`. Two-id delete lines repeat the same id; bulk-delete lines keep only the first id.
12. **Why unused `HDFS_templates.csv`?** 30 Loghub-style templates with different EventId numbers than the 2k structured file.
13. **What is in the Joblib file?** A dict: `pipeline`, `threshold`, `features`, `categorical`, `numeric`, `frequency_maps`, `settings`.
14. **If the results CSV is deleted?** `result_df()` calls `train_and_evaluate()` and rebuilds artifacts. A corrupt CSV can hit the `load_and_prepare()` path and break `/api/stats`.
15. **What if charts fail?** Chart.js is bundled at `/static/chart.umd.min.js`. KPI cards and tables still use the APIs if a canvas fails. Google Fonts need network; Segoe UI is the fallback.
