from __future__ import annotations

import json
import sys
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from anomaly_pipeline import load_and_prepare, train_and_evaluate  # noqa: E402
from hdfs_full_store import fetch_block, fetch_logs, stats as hdfs_full_stats  # noqa: E402

app = FastAPI(title="AI Log Anomaly Detection API", version="1.0")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.mount("/static", StaticFiles(directory=ROOT / "dashboard"), name="static")


def result_df():
    try:
        import pandas as pd
        results = ROOT / "results" / "anomaly_results.csv"
        if not results.exists():
            train_and_evaluate()
        return pd.read_csv(results)
    except Exception:
        return load_and_prepare()


@app.get("/", response_class=HTMLResponse)
def home():
    return (ROOT / "dashboard" / "index.html").read_text(encoding="utf-8")


@app.get("/api/health")
def health():
    return {"status": "ok", "service": "AI Log Anomaly Detection"}


@app.get("/api/stats")
def stats():
    df = result_df()
    detected = int(df["anomaly_prediction"].sum())
    total = int(len(df))
    actual = int((df["Label"] == "Anomaly").sum())
    return {
        "total_logs": total,
        "detected_anomalies": detected,
        "actual_anomalies": actual,
        "anomaly_rate": round((detected / total) * 100, 2) if total else 0,
        "event_types": int(df["EventId"].nunique()),
        "components": int(df["Component"].nunique()),
    }


@app.get("/api/anomalies")
def anomalies(limit: int = Query(20, ge=1, le=200)):
    df = result_df().sort_values("anomaly_score", ascending=False).head(limit)
    records = df[["LineId", "BlockId", "EventId", "Level", "Component", "Content", "anomaly_score", "Label"]].copy()
    records["anomaly_score"] = records["anomaly_score"].round(4)
    return records.fillna("").to_dict(orient="records")


@app.post("/api/retrain")
def retrain():
    _, metrics = train_and_evaluate()
    return metrics


@app.get("/api/hdfs-full-metrics")
def hdfs_full_metrics():
    """Read-only measured HDFS_v1 results. Does not retrain or touch the 2k demo."""
    path = ROOT / "results" / "hdfs_full_metrics.json"
    if not path.exists():
        return {
            "available": False,
            "dataset": "HDFS_v1",
            "reason": "Full HDFS_v1 evaluation has not been run. Results file is missing.",
        }
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "available": False,
            "dataset": "HDFS_v1",
            "reason": "results/hdfs_full_metrics.json exists but could not be read.",
        }
    if not isinstance(data, dict):
        return {"available": False, "dataset": "HDFS_v1", "reason": "Unexpected metrics file format."}
    data["available"] = True
    return data


@app.get("/api/hdfs-full/stats")
def hdfs_full_corpus_stats():
    """Ground-truth corpus counts from parquet metadata + saved ingest stats."""
    try:
        return hdfs_full_stats()
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/hdfs-full/logs")
def hdfs_full_logs(
    after_line: int = Query(0, ge=0),
    limit: int = Query(50, ge=1, le=200),
    block_id: str = Query(""),
    label: str = Query(""),
    level: str = Query(""),
    component: str = Query(""),
    q: str = Query(""),
):
    """Keyset-paginated parquet rows. Label is ground truth, not a model score."""
    anomaly = None
    if label:
        lowered = label.strip().lower()
        if lowered in {"anomaly", "1", "true"}:
            anomaly = 1
        elif lowered in {"normal", "0", "false"}:
            anomaly = 0
        else:
            raise HTTPException(status_code=400, detail="label must be Anomaly or Normal.")
    try:
        return fetch_logs(
            after_line=after_line,
            limit=limit,
            block_id=block_id,
            anomaly=anomaly,
            level=level,
            component=component,
            q=q,
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc


@app.get("/api/hdfs-full/blocks")
def hdfs_full_block(block_id: str = Query(..., min_length=3)):
    """All parquet lines for one BlockId, ground-truth labelled."""
    try:
        payload = fetch_block(block_id)
    except FileNotFoundError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    if not payload.get("found"):
        raise HTTPException(status_code=404, detail=f"No parquet rows for block_id={block_id}")
    return payload
