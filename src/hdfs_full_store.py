"""Paginated access to HDFS_v1 parquet without loading 11M rows into memory.

Queries use PyArrow dataset filters so row groups can be skipped via
``line_number`` min/max statistics. Original parquet files are never rewritten.
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, Dict, List, Optional

import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

ROOT = Path(__file__).resolve().parents[1]
METRICS_PATH = ROOT / "results" / "hdfs_full_metrics.json"
LOG_COLUMNS = [
    "line_number",
    "date",
    "time",
    "pid",
    "level",
    "component",
    "content",
    "block_id",
    "anomaly",
]


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


def _metrics() -> Dict[str, Any]:
    if not METRICS_PATH.exists():
        return {}
    try:
        data = json.loads(METRICS_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


@lru_cache(maxsize=1)
def parquet_dataset() -> ds.Dataset:
    return ds.dataset(str(resolve_parquet_dir()), format="parquet")


@lru_cache(maxsize=1)
def file_inventory() -> List[Dict[str, Any]]:
    items = []
    for path in sorted(resolve_parquet_dir().glob("*.parquet")):
        pf = pq.ParquetFile(path)
        first = pf.read_row_group(0, columns=["line_number"]).column(0)[0].as_py()
        last = pf.read_row_group(pf.num_row_groups - 1, columns=["line_number"]).column(0)[-1].as_py()
        items.append(
            {
                "name": path.name,
                "start_line": int(first),
                "end_line": int(last),
                "rows": int(pf.metadata.num_rows),
            }
        )
    return items


def corpus_row_count() -> int:
    return int(sum(item["rows"] for item in file_inventory()))


def _record(row: Dict[str, Any]) -> Dict[str, Any]:
    anomaly = int(row.get("anomaly") or 0)
    return {
        "LineId": int(row.get("line_number") or 0),
        "BlockId": "" if row.get("block_id") is None else str(row["block_id"]),
        "EventId": "—",
        "Level": "" if row.get("level") is None else str(row["level"]),
        "Component": "" if row.get("component") is None else str(row["component"]),
        "Content": "" if row.get("content") is None else str(row["content"]),
        "Date": "" if row.get("date") is None else str(row["date"]),
        "Time": "" if row.get("time") is None else str(row["time"]),
        "Pid": row.get("pid"),
        "Label": "Anomaly" if anomaly else "Normal",
        "anomaly": anomaly,
        "anomaly_score": None,
        "model_prediction": None,
        "label_source": "parquet.anomaly (block-level ground truth copied onto each line)",
        "prediction_source": "not_available",
    }


def _table_records(table) -> List[Dict[str, Any]]:
    payload = table.to_pydict()
    n = len(payload.get("line_number") or [])
    records = []
    for i in range(n):
        records.append(_record({key: payload[key][i] for key in payload}))
    return records


def _build_filter(
    *,
    after_line: int,
    block_id: str,
    anomaly: Optional[int],
    level: str,
    component: str,
    q: str,
):
    expr = ds.field("line_number") > after_line
    if block_id:
        expr = expr & (ds.field("block_id") == block_id)
    if anomaly is not None:
        expr = expr & (ds.field("anomaly") == int(anomaly))
    if level:
        expr = expr & (ds.field("level") == level)
    if component:
        expr = expr & (ds.field("component") == component)
    if q:
        text = (
            pc.match_substring(ds.field("content"), q, ignore_case=True)
            | pc.match_substring(ds.field("block_id"), q, ignore_case=True)
            | pc.match_substring(ds.field("component"), q, ignore_case=True)
            | pc.match_substring(ds.field("level"), q, ignore_case=True)
        )
        if q.isdigit():
            text = text | (ds.field("line_number") == int(q))
        expr = expr & text
    return expr


def fetch_logs(
    *,
    after_line: int = 0,
    limit: int = 50,
    block_id: str = "",
    anomaly: Optional[int] = None,
    level: str = "",
    component: str = "",
    q: str = "",
) -> Dict[str, Any]:
    limit = max(1, min(int(limit or 50), 200))
    after_line = max(int(after_line or 0), 0)
    block_id = (block_id or "").strip()
    level = (level or "").strip()
    component = (component or "").strip()
    q = (q or "").strip()
    expr = _build_filter(
        after_line=after_line,
        block_id=block_id,
        anomaly=anomaly,
        level=level,
        component=component,
        q=q,
    )
    table = parquet_dataset().scanner(filter=expr, columns=LOG_COLUMNS).head(limit + 1)
    records = _table_records(table)
    has_more = len(records) > limit
    page = records[:limit]
    return {
        "dataset": "HDFS_v1",
        "paging": {
            "strategy": "keyset_line_number_exclusive",
            "after_line": after_line,
            "limit": limit,
            "next_after_line": page[-1]["LineId"] if page else after_line,
            "has_more": has_more,
        },
        "filters": {
            "block_id": block_id or None,
            "anomaly": anomaly,
            "level": level or None,
            "component": component or None,
            "q": q or None,
        },
        "model_predictions_available": False,
        "prediction_note": (
            "Isolation Forest scores were not saved per HDFS_v1 log line. "
            "Label is parquet ground truth only."
        ),
        "records": page,
        "returned": len(page),
    }


def fetch_block(block_id: str, limit: int = 200) -> Dict[str, Any]:
    block_id = (block_id or "").strip()
    if not block_id:
        return {
            "dataset": "HDFS_v1",
            "block_id": "",
            "found": False,
            "reason": "block_id is required.",
            "records": [],
            "model_predictions_available": False,
        }
    page = fetch_logs(after_line=0, limit=limit, block_id=block_id)
    records = page["records"]
    label = None
    if records:
        labels = {rec["Label"] for rec in records}
        label = labels.pop() if len(labels) == 1 else "Mixed"
    return {
        "dataset": "HDFS_v1",
        "block_id": block_id,
        "found": bool(records),
        "ground_truth_label": label,
        "n_lines_returned": len(records),
        "has_more": page["paging"]["has_more"],
        "model_predictions_available": False,
        "prediction_note": (
            "No per-line Isolation Forest scores were saved for HDFS_v1. "
            "This payload is ground-truth parquet only."
        ),
        "records": records,
    }


def stats() -> Dict[str, Any]:
    metrics = _metrics()
    ingest = metrics.get("ingest") if isinstance(metrics.get("ingest"), dict) else {}
    split = metrics.get("split") if isinstance(metrics.get("split"), dict) else {}
    improved = (
        metrics.get("improved_held_out_blocks")
        if isinstance(metrics.get("improved_held_out_blocks"), dict)
        else {}
    )
    total = corpus_row_count()
    unique_blocks = ingest.get("unique_blocks")
    anomalous_blocks = ingest.get("anomalous_blocks")
    block_rate = None
    if unique_blocks and anomalous_blocks is not None:
        block_rate = round((float(anomalous_blocks) / float(unique_blocks)) * 100, 2)
    return {
        "dataset": "HDFS_v1",
        "available": True,
        "total_logs": total,
        "unique_blocks": unique_blocks,
        "actual_anomalous_blocks": anomalous_blocks,
        "block_anomaly_rate": block_rate,
        "event_types": ingest.get("unique_event_ids_raw"),
        "model_predictions_available": False,
        "detected_anomalies": None,
        "anomaly_rate_note": (
            "block_anomaly_rate is ground-truth anomalous blocks ÷ unique blocks. "
            "It is not Isolation Forest detection rate."
        ),
        "counts_source": {
            "total_logs": "parquet file metadata",
            "unique_blocks": "results/hdfs_full_metrics.json ingest",
            "actual_anomalous_blocks": "results/hdfs_full_metrics.json ingest",
        },
        "held_out": {
            "evaluation": metrics.get("evaluation"),
            "n_test_blocks": split.get("n_test_blocks"),
            "n_test_anomalies": split.get("n_test_anomalies"),
            "f1": improved.get("f1"),
            "pr_auc": improved.get("pr_auc"),
        },
        "parquet_dir": str(resolve_parquet_dir()),
        "files": file_inventory(),
    }
