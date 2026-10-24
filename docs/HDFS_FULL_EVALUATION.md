# HDFS_v1 full-corpus evaluation

This document reports a **measured** Isolation Forest evaluation on the official HDFS_v1 parquet under `data/HDFS_full`. It does not replace the HDFS-2k demo.

**Sources of truth:** `results/hdfs_full_metrics.json` (full run), `results/hdfs_full_subset_metrics.json` (smoke test), `src/hdfs_full_eval.py`.  
**Not overwritten:** `results/anomaly_results.csv`, `results/metrics.json`, `artifacts/isolation_forest_pipeline.joblib`.  
**Unchanged API contracts:** `/api/health`, `/api/stats`, `/api/anomalies`, `/api/retrain` still serve HDFS-2k.

## Dataset schema (inspected)

Parquet files: `data/HDFS_full/data/train-00000-of-00005.parquet` … `train-00004-of-00005.parquet`.

| Column | Role |
|---|---|
| `line_number` | Source line index. Unused by the model. |
| `date`, `time`, `pid` | Timestamp / process. Hour is parsed from `time`. |
| `level`, `component`, `content` | Same semantics as HDFS-2k structured fields. |
| `block_id` | Ground-truth grouping key. Space-separated ids keep the first `blk_…` only. |
| `anomaly` | Binary block label (`int8`). Evaluation uses the max flag per BlockId. |

There is no Drain `EventId` in this dump. EventId is `E` plus the first 8 hex chars of an MD5 over content after masking BlockIds, IPs, and digits.

## Protocol

1. Stream parquet **row groups** (not a single in-memory 11M-row frame).
2. Aggregate incrementally to one row per unique `block_id`.
3. Split unique BlockIds 60 / 20 / 20, stratified by the block label, `random_state=42`.
4. Fit event/component frequency maps on **train blocks only**.
5. **Baseline:** Isolation Forest, original feature set, `contamination=0.05`, fit on train blocks, `predict() == -1`.
6. **Improved:** reuse the HDFS-2k validation-selected settings (`train_on=normal`, `n_estimators=200`, `max_samples=auto`, `contamination=auto`). Threshold is chosen on validation F1, then precision.
7. Official metrics are **held-out test blocks**: confusion matrix, precision, recall, F1, false-positive rate, and PR-AUC (`average_precision_score` on Isolation Forest scores).

The 8-config HDFS-2k grid was **not** rerun on the full corpus.

## Smoke test (80,000 rows, measured)

From `results/hdfs_full_subset_metrics.json` after `python src/hdfs_full_eval.py --subset`:

- 80,000 lines → 6,386 blocks → 227 anomalies (0 mixed-label blocks).
- Split: 3,831 / 1,277 / 1,278 blocks (136 / 45 / 46 anomalies).
- Early-file EventId cardinality was only 4 templates; later files contain more event types.

**Baseline, held-out (1,278 test blocks):**  
precision 0.2763, recall 0.4565, F1 0.3443, FPR 0.0446, PR-AUC 0.4740, TP/TN/FP/FN = 21/1177/55/25.

**Improved, held-out (same 1,278 test blocks):**  
precision 0.5263, recall 0.4348, F1 0.4762, FPR 0.0146, PR-AUC 0.4164, TP/TN/FP/FN = 20/1214/18/26.

On this prefix, improved F1 is higher (+0.1319) but baseline PR-AUC is higher. That is a measured ranking-vs-threshold disagreement, not a claim that one method dominates.

This prefix is **not** the official full-corpus result. The first 80k lines are early INFO-only traffic and under-represent later delete/replicate templates.

## Full corpus (measured)

From `results/hdfs_full_metrics.json` after `python src/hdfs_full_eval.py` (244.9 s wall clock, 215.3 s streaming):

- 11,175,629 lines → 575,061 blocks → 16,838 anomalies (2.93%).
- 0 empty `block_id` rows, 0 mixed-label blocks. This parquet dump stores one `block_id` per row.
- 12 hashed EventIds after masking (capping at 64 was not triggered).
- Split: 345,036 / 115,012 / 115,013 blocks (10,103 / 3,367 / 3,368 anomalies).
- Improved threshold `0.129092` chosen on validation score quantiles (383 candidates).

**Baseline, held-out (115,013 test blocks):**  
precision 0.3215, recall 0.5582, F1 0.4080, FPR 0.0355, PR-AUC 0.4652, TP/TN/FP/FN = 1880/107678/3967/1488.

**Improved, held-out (same 115,013 test blocks):**  
precision 0.3181, recall 0.5736, F1 0.4093, FPR 0.0371, PR-AUC 0.2370, TP/TN/FP/FN = 1932/107504/4141/1436.

**Comparison:** F1 delta is +0.0012. That is a tie, not a meaningful improvement. Baseline PR-AUC is higher (0.465 vs 0.237), so the contamination=0.05 baseline ranks test anomalies better. The HDFS-2k settings are **not** claimed as a full-corpus upgrade.

The Model Evaluation page loads this file through `GET /api/hdfs-full-metrics`. Numbers are not hard-coded in the dashboard.

## How to reproduce

From the `ready` directory, with `.venv` active:

```powershell
python src/hdfs_full_eval.py --subset
python src/hdfs_full_eval.py
```

The dashboard does not run the full experiment. Retrain remains the HDFS-2k trainer.

## Limitations

- EventId is a masked-content hash, not the official Loghub Drain template id.
- Multi-block lines keep only the first BlockId (same convention as the 2k demo).
- EventId one-hot width is capped at 64 values fit on train blocks.
- Large validation splits use a quantile threshold search, not every unique score.
- Isolation Forest is unsupervised. Labels are used only to drop known anomalies when `train_on=normal`, to pick the validation threshold, and to score the test split.
- The live Overview / Live Logs / Alerts / Investigation views still show HDFS-2k.
