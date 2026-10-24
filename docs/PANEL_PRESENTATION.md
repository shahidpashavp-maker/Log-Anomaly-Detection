# Panel presentation report — Log Anomaly Detection

**Status:** Ready to present.  
**Repo:** https://github.com/Gladiator2006-fz/Log-Anomaly-Detection  
**Demo:** http://127.0.0.1:8000 after `uvicorn` from this `ready` folder.  
**Sources of truth:** `results/metrics.json` and live `/api/stats`. Do not invent extra decimals.

This is a local HDFS_2k + Isolation Forest + FastAPI demo. It is not a production SOC, not real-time streaming, and not a confirmed-incident system.

---

## 1. One-minute thesis

HDFS labels are **block-level**. The original trainer fitted and scored all 2,000 lines, which leaks evaluation data. This project:

1. Splits **BlockIds** 60 / 20 / 20, stratified.
2. Fits Isolation Forest on **train** (normal-only won validation).
3. Chooses the score threshold on **validation only**.
4. Reports official P / R / F1 / FPR on **399 held-out test blocks**.
5. Shows **corpus** flags (7 of 2,000) on the dashboard as a separate statistic.

Official test result: **precision 0.5000, recall 0.0769, F1 0.1333, FPR 0.0026** (TP/FP/FN = 1/1/12). Recall is low because HDFS_2k has about one line per block, so sequence signals are missing.

---

## 2. Numbers to quote (do not mix them)

| What you are asked | Say this | Do not say |
|---|---|---|
| Official academic result | Held-out test blocks: P 0.50, R 0.077, F1 0.133, FPR 0.0026 | “The model is 0.35% accurate” |
| Dashboard KPI | 2,000 lines, **7 model-flagged** records, 0.35% corpus rate | “7 confirmed incidents” |
| Ground truth in the sample | 69 lines with Label = Anomaly | That 69 is the model output |
| Baseline comparison | In-sample F1 0.036 → fair held-out F1 0.065 → improved F1 0.133 | That F1 is high |
| Alerts page | 7 candidate anomalies from `anomaly_prediction` | That the table is the complete flagged set |

`/api/anomalies` returns **highest scores**, not a filter on `anomaly_prediction=1`. The Alerts table is a sample. Totals come from `/api/stats`.

---

## 3. System the panel will see

Nexora Observability is a **fictional demo tenant**. Sign-in is mock (`aarav.mehta@nexora.example` / `nexora-enterprise`, or **Enter Demo Workspace**).

Pages: Overview, Live Logs, Alerts & Detection Center, Anomaly Investigation, Model Evaluation, Activity History.

- **LIVE** means `/api/health` is ok. It is not a live tail.
- **Retrain** calls `POST /api/retrain` and waits. There is no progress-stream API; the overlay is a wait state.
- Held-out P/R/F1 appear on Model Evaluation **after** a successful retrain in that browser session.

---

## 4. 8-minute demo script

1. **Launch** (already running if possible). `python -m uvicorn app.main:app --host 127.0.0.1 --port 8000`
2. **Login.** Click Enter Demo Workspace. State: mock auth, fictional org.
3. **Overview (90 s).** Point at 2,000 / 7 / 0.35%. Say: corpus flags, not test F1. **LIVE** = API connected.
4. **Problem (60 s).** Labels are per BlockId. Fitting on all 2,000 lines is in-sample. We split blocks, train-only frequencies, validation threshold.
5. **Alerts (60 s).** 7 model-flagged vs 69 labelled. Candidates, not incidents. Open **Review flagged logs**.
6. **Retrain (wait ~10–60 s).** Show overlay stages, then toast with **7** flags. Open **Model Evaluation**: 0.5000 / 0.0769 / 0.1333 / 0.0026.
7. **Limitation (45 s).** Recall is 1 of 13 because this file has ~1 line/block. Isolation Forest isolated rare `E14` verification lines. Future work is full HDFS traces / sequence models — not implemented.
8. **Stop.** Offer Q&A.

If retrain is slow, keep talking through the split and why 0.35% is not F1.

---

## 5. Setup if the demo machine is cold

```powershell
cd path\to\ready
.\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
python -m uvicorn app.main:app --host 127.0.0.1 --port 8000
```

Open http://127.0.0.1:8000  
If artifacts are missing: `python src/train_model.py` first.

Keep `results/anomaly_results.csv` intact. A corrupt CSV can break `/api/stats`.

---

## 6. Honest answers (short)

- **Why is F1 only 0.13?** Modest gain over a fair baseline of 0.065. Recall stayed 1/13.
- **Did training fail?** No. The dataset lacks per-block event sequences.
- **Is Isolation Forest supervised?** No. Labels are not `y`. They filter training when `train_on=normal`, pick the validation threshold, and score metrics.
- **Why evaluate blocks?** `anomaly_label.csv` is one label per BlockId.
- **What is 0.35%?** `7 / 2000` corpus flags. Not F1, not recall.
- **Can you stream logs?** No. Explorer max is 200 scored API rows.
- **Nexora?** Fictional university demo organisation.

---

## 7. What was verified for this audit

- Live `GET /api/health` → `ok`
- Live `GET /api/stats` → 2000 / 7 / 69 / 0.35% / 14 events / 6 components
- Official metrics in `results/metrics.json` match the handoff table
- Saved model: `artifacts/isolation_forest_pipeline.joblib`
- GitHub `main` contains the project (`9d60327` at last push)
- Chart.js is now served from `/static/chart.umd.min.js` (no CDN for charts)

---

## 8. Known limits (say them; do not hide them)

- Low recall on HDFS_2k; small test anomaly count (13 blocks)
- Dashboard KPIs ≠ academic F1
- Retrain JSON mixes corpus counts and held-out metrics (keys are documented)
- Mock login, local-only, no Docker/cloud
- Google Fonts still load from the network; UI falls back to Segoe UI if that fails
