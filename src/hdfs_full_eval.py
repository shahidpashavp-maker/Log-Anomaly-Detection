"""Leakage-safe Isolation Forest evaluation on the full HDFS_v1 parquet.

This script is separate from the HDFS_2k demo path. It does not overwrite
`results/anomaly_results.csv`, `results/metrics.json`, or the saved 2k Joblib
bundle. The dashboard, `/api/stats`, `/api/anomalies`, and `/api/retrain`
continue to serve the 2k corpus.

Processing stays at block granularity after a streaming pass over parquet row
groups so the 11M-line corpus is never loaded as one DataFrame.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
from collections import Counter
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import numpy as np
import pandas as pd
from sklearn.metrics import average_precision_score

ROOT = Path(__file__).resolve().parents[1]
SRC = Path(__file__).resolve().parent
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from anomaly_pipeline import (  # noqa: E402
    BASELINE_FEATURES,
    BLOCK_RE,
    FEATURES,
    IP_RE,
    SPLIT_RANDOM_STATE,
    TEST_FRAC,
    TRAIN_FRAC,
    VAL_FRAC,
    anomaly_scores,
    apply_frequency_maps,
    build_pipeline,
    classification_metrics,
    frequency_maps,
    split_block_ids,
    tune_threshold,
)

EVENT_ID_CAP = 64
THRESHOLD_QUANTILES = 401
IMPROVED_SETTINGS = {
    "train_on": "normal",
    "n_estimators": 200,
    "max_samples": "auto",
    "contamination": "auto",
    "reason": (
        "Reused the HDFS_2k validation-selected Isolation Forest settings. "
        "An 8-config grid on hundreds of thousands of train blocks was not rerun."
    ),
}

MASK_BLOCK = BLOCK_RE
MASK_IP = IP_RE
MASK_NUM = __import__("re").compile(r"\d+")


def resolve_parquet_dir() -> Path:
    candidates = [
        ROOT / "data" / "HDFS_full" / "data",
        ROOT.parent / "data" / "HDFS_full" / "data",
    ]
    for path in candidates:
        if path.is_dir() and any(path.glob("*.parquet")):
            return path
    tried = ", ".join(str(p) for p in candidates)
    raise FileNotFoundError(f"No HDFS_v1 parquet files found. Looked in: {tried}")


def first_block_id(raw: Any) -> str:
    """Keep the first BlockId when a line mentions several, matching the 2k demo."""
    text = "" if raw is None else str(raw).strip()
    if not text:
        return ""
    token = text.split(" ", 1)[0]
    if token.startswith("blk_"):
        return token
    match = BLOCK_RE.search(text)
    return match.group(0) if match else ""


def event_id_from_content(content: str) -> str:
    """Stable EventId from a masked message. The parquet has no Drain EventId."""
    masked = MASK_BLOCK.sub("<BLK>", content)
    masked = MASK_IP.sub("<IP>", masked)
    masked = MASK_NUM.sub("<N>", masked)
    digest = hashlib.md5(masked.encode("utf-8", "ignore")).hexdigest()[:8]
    return f"E{digest}"


def parse_hour(value: Any) -> int:
    text = str(value or "").zfill(6)
    try:
        hour = int(text[:2])
    except ValueError:
        return 0
    return hour if 0 <= hour <= 23 else 0


def _new_block() -> Dict[str, Any]:
    return {
        "y": 0,
        "n_lines": 0,
        "message_length": 0.0,
        "token_count": 0.0,
        "digit_count": 0.0,
        "ip_count": 0.0,
        "hour": 0.0,
        "has_exception": 0,
        "has_warning_word": 0,
        "is_delete": 0,
        "is_replicate": 0,
        "is_verify": 0,
        "is_served": 0,
        "is_warn": 0,
        "block_mentions": 0,
        "levels": Counter(),
        "components": Counter(),
        "events": Counter(),
        "seen_labels": 0,
    }


def _update_block(
    acc: Dict[str, Any],
    *,
    level: str,
    component: str,
    content: str,
    anomaly: int,
    hour: int,
) -> None:
    low = content.lower()
    event_id = event_id_from_content(content)
    label = 1 if int(anomaly) else 0
    if acc["n_lines"] and acc["y"] != label:
        acc["seen_labels"] = 1
    acc["y"] = max(acc["y"], label)
    acc["n_lines"] += 1
    acc["message_length"] += float(len(content))
    acc["token_count"] += float(len(content.split()))
    acc["digit_count"] += float(sum(ch.isdigit() for ch in content))
    acc["ip_count"] += float(len(IP_RE.findall(content)))
    acc["hour"] += float(hour)
    acc["has_exception"] = max(
        acc["has_exception"], int(any(w in low for w in ("exception", "error", "failed", "interrupted")))
    )
    acc["has_warning_word"] = max(acc["has_warning_word"], int("warn" in low))
    acc["is_delete"] = max(acc["is_delete"], int("delete" in low or "invalidset" in low))
    acc["is_replicate"] = max(acc["is_replicate"], int("replicate" in low or "transfer" in low))
    acc["is_verify"] = max(acc["is_verify"], int("verification" in low))
    acc["is_served"] = max(acc["is_served"], int("served block" in low or "serving" in low))
    acc["is_warn"] = max(acc["is_warn"], int(level == "WARN"))
    acc["block_mentions"] = max(acc["block_mentions"], len(BLOCK_RE.findall(content)))
    acc["levels"][level or "UNKNOWN"] += 1
    acc["components"][component or "UNKNOWN"] += 1
    acc["events"][event_id] += 1


def stream_blocks(
    parquet_dir: Path,
    max_lines: Optional[int] = None,
) -> Tuple[pd.DataFrame, Dict[str, Any]]:
    import pyarrow.parquet as pq

    files = sorted(parquet_dir.glob("*.parquet"))
    if not files:
        raise FileNotFoundError(f"No parquet files in {parquet_dir}")

    blocks: Dict[str, Dict[str, Any]] = {}
    n_rows = 0
    n_empty = 0
    n_multi = 0
    n_mixed = 0
    started = time.perf_counter()

    for path in files:
        pf = pq.ParquetFile(path)
        for rg in range(pf.num_row_groups):
            table = pf.read_row_group(
                rg, columns=["time", "level", "component", "content", "block_id", "anomaly"]
            )
            times = table.column("time").to_pylist()
            levels = table.column("level").to_pylist()
            components = table.column("component").to_pylist()
            contents = table.column("content").to_pylist()
            block_ids = table.column("block_id").to_pylist()
            anomalies = table.column("anomaly").to_pylist()

            for time_v, level, component, content, raw_bid, anomaly in zip(
                times, levels, components, contents, block_ids, anomalies
            ):
                n_rows += 1
                raw_text = "" if raw_bid is None else str(raw_bid)
                if " " in raw_text:
                    n_multi += 1
                bid = first_block_id(raw_text)
                if not bid:
                    n_empty += 1
                    if max_lines and n_rows >= max_lines:
                        break
                    continue
                acc = blocks.get(bid)
                if acc is None:
                    acc = _new_block()
                    blocks[bid] = acc
                _update_block(
                    acc,
                    level="" if level is None else str(level),
                    component="" if component is None else str(component),
                    content="" if content is None else str(content),
                    anomaly=int(anomaly or 0),
                    hour=parse_hour(time_v),
                )
                if max_lines and n_rows >= max_lines:
                    break
            if max_lines and n_rows >= max_lines:
                break
        if max_lines and n_rows >= max_lines:
            break

    rows: List[Dict[str, Any]] = []
    for bid, acc in blocks.items():
        n = max(int(acc["n_lines"]), 1)
        if acc["seen_labels"]:
            n_mixed += 1
        level = acc["levels"].most_common(1)[0][0] if acc["levels"] else "UNKNOWN"
        component = acc["components"].most_common(1)[0][0] if acc["components"] else "UNKNOWN"
        event_id = acc["events"].most_common(1)[0][0] if acc["events"] else "E00000000"
        rows.append(
            {
                "BlockId": bid,
                "y": int(acc["y"]),
                "Label": "Anomaly" if acc["y"] else "Normal",
                "Level": level,
                "Component": component,
                "EventId": event_id,
                "message_length": acc["message_length"] / n,
                "token_count": acc["token_count"] / n,
                "digit_count": acc["digit_count"] / n,
                "ip_count": acc["ip_count"] / n,
                "hour": acc["hour"] / n,
                "has_exception": int(acc["has_exception"]),
                "has_warning_word": int(acc["has_warning_word"]),
                "is_delete": int(acc["is_delete"]),
                "is_replicate": int(acc["is_replicate"]),
                "is_verify": int(acc["is_verify"]),
                "is_served": int(acc["is_served"]),
                "is_warn": int(acc["is_warn"]),
                "block_mentions": int(acc["block_mentions"]),
                "n_lines": int(acc["n_lines"]),
                "n_events": int(len(acc["events"])),
            }
        )

    frame = pd.DataFrame(rows)
    stats = {
        "parquet_dir": str(parquet_dir),
        "parquet_files": [p.name for p in files],
        "rows_read": int(n_rows),
        "empty_block_id_rows": int(n_empty),
        "multi_block_id_rows": int(n_multi),
        "unique_blocks": int(len(frame)),
        "anomalous_blocks": int(frame["y"].sum()) if len(frame) else 0,
        "mixed_label_blocks": int(n_mixed),
        "unique_event_ids_raw": int(frame["EventId"].nunique()) if len(frame) else 0,
        "stream_seconds": float(time.perf_counter() - started),
        "truncated": bool(max_lines and n_rows >= max_lines),
        "max_lines": max_lines,
    }
    return frame, stats


def _select_rows(df: pd.DataFrame, block_ids: Iterable[Any]) -> pd.DataFrame:
    return df[df["BlockId"].isin(set(block_ids))].copy()


def cap_event_ids(
    train_blocks: pd.DataFrame, *others: pd.DataFrame, cap: int = EVENT_ID_CAP
) -> Tuple[pd.DataFrame, ...]:
    """Limit one-hot width. Cap is fit on train EventIds only."""
    n_unique = int(train_blocks["EventId"].nunique())
    frames = [train_blocks, *others]
    if n_unique <= cap:
        return tuple(frames)
    keep = set(train_blocks["EventId"].value_counts().head(cap - 1).index)
    capped = []
    for frame in frames:
        out = frame.copy()
        out["EventId"] = out["EventId"].where(out["EventId"].isin(keep), "E_OTHER")
        capped.append(out)
    return tuple(capped)


def tune_threshold_quantiles(scores: np.ndarray, y_true: np.ndarray) -> Tuple[float, Dict[str, Any]]:
    """F1-then-precision sweep over score quantiles. Full unique-score loops are too large."""
    y_true = np.asarray(y_true, dtype=int)
    scores = np.asarray(scores, dtype=float)
    if not len(scores):
        return 0.0, classification_metrics(y_true, np.zeros_like(y_true))

    qs = np.linspace(0.50, 0.999, THRESHOLD_QUANTILES)
    candidates = np.unique(np.quantile(scores, qs))
    best: Dict[str, Any] | None = None
    best_threshold = float(np.quantile(scores, 0.95))
    for threshold in candidates:
        pred = (scores >= threshold).astype(int)
        metrics = classification_metrics(y_true, pred)
        better = False
        if best is None:
            better = True
        elif metrics["f1"] > best["f1"]:
            better = True
        elif metrics["f1"] == best["f1"] and metrics["precision"] > best["precision"]:
            better = True
        if better:
            best = metrics
            best_threshold = float(threshold)
    assert best is not None
    best["threshold"] = best_threshold
    best["threshold_search"] = "validation_score_quantiles"
    best["n_candidates"] = int(len(candidates))
    return best_threshold, best


def pr_auc(y_true: Iterable[int], scores: np.ndarray) -> Optional[float]:
    y = np.asarray(y_true, dtype=int)
    s = np.asarray(scores, dtype=float)
    if y.sum() == 0 or len(np.unique(y)) < 2:
        return None
    return float(average_precision_score(y, s))


def _attach_ranking(metrics: Dict[str, Any], y_true: Iterable[int], scores: np.ndarray) -> Dict[str, Any]:
    out = dict(metrics)
    auc = pr_auc(y_true, scores)
    out["pr_auc"] = auc
    out["pr_auc_note"] = (
        "Average precision on Isolation Forest scores (higher = better ranking). "
        "Not defined when a split has only one class."
        if auc is None
        else "Average precision on Isolation Forest scores (threshold-independent)."
    )
    return out


def evaluate_blocks(blocks: pd.DataFrame, ingest: Dict[str, Any], scope: str) -> Dict[str, Any]:
    started = time.perf_counter()
    if blocks.empty:
        raise ValueError("No blocks aggregated from HDFS_v1.")

    train_ids, val_ids, test_ids = split_block_ids(blocks)
    train_blocks = _select_rows(blocks, train_ids)
    val_blocks = _select_rows(blocks, val_ids)
    test_blocks = _select_rows(blocks, test_ids)
    train_blocks, val_blocks, test_blocks = cap_event_ids(train_blocks, val_blocks, test_blocks)

    maps = frequency_maps(train_blocks)
    train_blocks = apply_frequency_maps(train_blocks, maps)
    val_blocks = apply_frequency_maps(val_blocks, maps)
    test_blocks = apply_frequency_maps(test_blocks, maps)

    baseline_pipe = build_pipeline(
        categorical=["Level", "Component", "EventId"],
        numeric=[
            "message_length",
            "token_count",
            "digit_count",
            "ip_count",
            "has_exception",
            "has_warning_word",
            "hour",
            "event_frequency",
            "component_frequency",
            "event_component_frequency",
        ],
        n_estimators=200,
        contamination=0.05,
        max_samples="auto",
    )
    baseline_pipe.fit(train_blocks[BASELINE_FEATURES])
    baseline_pred = (baseline_pipe.predict(test_blocks[BASELINE_FEATURES]) == -1).astype(int)
    baseline_scores = anomaly_scores(baseline_pipe, test_blocks, BASELINE_FEATURES)
    baseline = _attach_ranking(
        classification_metrics(test_blocks["y"].to_numpy(), baseline_pred),
        test_blocks["y"],
        baseline_scores,
    )
    baseline["scope"] = "held_out_test_blocks"
    baseline["protocol"] = "original_features_contamination_0.05_fit_train"

    fit_df = train_blocks[train_blocks["y"] == 0]
    if fit_df.empty:
        fit_df = train_blocks
        train_on = "all"
    else:
        train_on = IMPROVED_SETTINGS["train_on"]

    improved_pipe = build_pipeline(
        n_estimators=int(IMPROVED_SETTINGS["n_estimators"]),
        contamination=IMPROVED_SETTINGS["contamination"],
        max_samples=IMPROVED_SETTINGS["max_samples"],
    )
    improved_pipe.fit(fit_df[FEATURES])
    val_scores = anomaly_scores(improved_pipe, val_blocks, FEATURES)
    if len(val_blocks) <= 20_000:
        threshold, val_metrics = tune_threshold(val_scores, val_blocks["y"].to_numpy())
        val_metrics["threshold_search"] = "all_unique_validation_scores"
    else:
        threshold, val_metrics = tune_threshold_quantiles(val_scores, val_blocks["y"].to_numpy())
    val_metrics = _attach_ranking(val_metrics, val_blocks["y"], val_scores)

    test_scores = anomaly_scores(improved_pipe, test_blocks, FEATURES)
    test_pred = (test_scores >= threshold).astype(int)
    improved = _attach_ranking(
        classification_metrics(test_blocks["y"].to_numpy(), test_pred),
        test_blocks["y"],
        test_scores,
    )
    improved["scope"] = "held_out_test_blocks"
    improved["protocol"] = "hdfs2k_settings_plus_validation_threshold"

    elapsed = time.perf_counter() - started
    f1_delta = float(improved["f1"] - baseline["f1"])
    report = {
        "available": True,
        "dataset": "HDFS_v1",
        "evaluation": "held_out_test_blocks",
        "scope": scope,
        "ingest": ingest,
        "split": {
            "unit": "BlockId / block_id",
            "stratified_by": "block anomaly label",
            "random_state": SPLIT_RANDOM_STATE,
            "train_frac": TRAIN_FRAC,
            "val_frac": VAL_FRAC,
            "test_frac": TEST_FRAC,
            "n_train_blocks": int(len(train_ids)),
            "n_val_blocks": int(len(val_ids)),
            "n_test_blocks": int(len(test_ids)),
            "n_train_anomalies": int(train_blocks["y"].sum()),
            "n_val_anomalies": int(val_blocks["y"].sum()),
            "n_test_anomalies": int(test_blocks["y"].sum()),
            "event_id_unique_train": int(train_blocks["EventId"].nunique()),
            "event_id_capped_at": EVENT_ID_CAP,
        },
        "settings": {
            "model": "IsolationForest",
            "train_on": train_on,
            "n_estimators": IMPROVED_SETTINGS["n_estimators"],
            "max_samples": IMPROVED_SETTINGS["max_samples"],
            "contamination": IMPROVED_SETTINGS["contamination"],
            "decision_threshold": float(threshold),
            "threshold_objective": "max validation F1, then precision",
            "frequency_maps_fit_on": "train blocks only",
            "event_id": "md5 of content after masking BlockId, IP, and digits",
            "settings_source": IMPROVED_SETTINGS["reason"],
            "features": FEATURES,
            "labels_used_for": [
                "excluding known anomalies from IF training when train_on=normal",
                "validation threshold selection",
                "held-out evaluation",
            ],
        },
        "baseline_held_out_blocks": baseline,
        "improved_validation_blocks": val_metrics,
        "improved_held_out_blocks": improved,
        "comparison": {
            "metric": "held_out_block_f1",
            "baseline_f1": baseline["f1"],
            "improved_f1": improved["f1"],
            "f1_delta": f1_delta,
            "improved_wins_f1": bool(f1_delta > 0),
            "baseline_pr_auc": baseline.get("pr_auc"),
            "improved_pr_auc": improved.get("pr_auc"),
            "note": (
                "Improved uses the 2k-selected Isolation Forest settings plus a "
                "validation threshold. Baseline uses contamination=0.05 on the "
                "original feature set, fit on train blocks only. Deltas are "
                "measured, not assumed."
            ),
        },
        "processing_time_sec": float(elapsed + float(ingest.get("stream_seconds", 0.0))),
        "fit_time_sec": float(elapsed),
        "limitations": [
            "HDFS_v1 parquet rows have no official Drain EventId; EventId is a masked-content hash.",
            "Lines that mention several blocks keep only the first BlockId, matching the 2k demo.",
            "Block label is the max of the parquet `anomaly` flag (1 if any line in the block is labelled anomalous).",
            "The 8-config Isolation Forest grid from HDFS_2k was not rerun on the full corpus.",
            "Validation thresholds on large splits use score quantiles, not every unique score.",
            "EventId one-hot width is capped at 64 values fit on train blocks only.",
            "Isolation Forest is unsupervised; labels are not used as a supervised training target.",
            "The live dashboard, /api/stats, /api/anomalies, and /api/retrain still serve HDFS_2k only.",
            "A small held-out F1 delta is not a claim of improvement. Compare PR-AUC as well; the two can disagree.",
            "Masked-content hashing can collapse official Drain templates into fewer EventIds.",
        ],
    }
    return report


def default_output(scope: str) -> Path:
    name = "hdfs_full_subset_metrics.json" if scope == "subset" else "hdfs_full_metrics.json"
    return ROOT / "results" / name


def run(max_lines: Optional[int] = None, output: Optional[Path] = None) -> Dict[str, Any]:
    parquet_dir = resolve_parquet_dir()
    scope = "subset" if max_lines else "full"
    print(f"[hdfs_full] streaming {scope} from {parquet_dir}", flush=True)
    blocks, ingest = stream_blocks(parquet_dir, max_lines=max_lines)
    print(
        f"[hdfs_full] rows={ingest['rows_read']} blocks={ingest['unique_blocks']} "
        f"anomalies={ingest['anomalous_blocks']} stream_s={ingest['stream_seconds']:.1f}",
        flush=True,
    )
    report = evaluate_blocks(blocks, ingest, scope=scope)
    path = Path(output) if output else default_output(scope)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report, indent=2), encoding="utf-8")
    improved = report["improved_held_out_blocks"]
    baseline = report["baseline_held_out_blocks"]
    print(f"[hdfs_full] wrote {path}", flush=True)
    print(
        "[hdfs_full] improved test "
        f"P={improved['precision']:.4f} R={improved['recall']:.4f} "
        f"F1={improved['f1']:.4f} FPR={improved['false_positive_rate']:.4f} "
        f"PR-AUC={improved.get('pr_auc')} "
        f"TP/FP/FN={improved['tp']}/{improved['fp']}/{improved['fn']}",
        flush=True,
    )
    print(
        "[hdfs_full] baseline test "
        f"P={baseline['precision']:.4f} R={baseline['recall']:.4f} "
        f"F1={baseline['f1']:.4f} FPR={baseline['false_positive_rate']:.4f} "
        f"PR-AUC={baseline.get('pr_auc')}",
        flush=True,
    )
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description="Evaluate Isolation Forest on HDFS_v1")
    parser.add_argument("--max-lines", type=int, default=None, help="Read at most this many parquet rows")
    parser.add_argument("--subset", action="store_true", help="Smoke test on 80,000 rows")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    max_lines = args.max_lines
    if args.subset and max_lines is None:
        max_lines = 80_000
    run(max_lines=max_lines, output=args.output)


if __name__ == "__main__":
    main()
