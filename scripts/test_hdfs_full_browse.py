"""Verify HDFS_v1 parquet browse + existing 2k demo routes.

Run from the ready/ directory:
  .\\.venv\\Scripts\\python.exe scripts\\test_hdfs_full_browse.py
"""
from __future__ import annotations

import json
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from hdfs_full_store import (  # noqa: E402
    fetch_block,
    fetch_logs,
    resolve_parquet_dir,
    stats as full_stats,
)

BASE = "http://127.0.0.1:8000"
FAILED = []


def check(name: str, cond: bool, detail: str = "") -> None:
    if cond:
        print("PASS  " + name)
    else:
        FAILED.append(name)
        print("FAIL  " + name + (("  " + detail) if detail else ""))


def get_json(path: str, timeout: int = 60):
    req = urllib.request.Request(BASE + path)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            body = json.loads(resp.read().decode("utf-8"))
            return resp.status, body
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            payload = {"raw": raw}
        return exc.code, payload


def store_tests() -> None:
    parquet_dir = resolve_parquet_dir()
    files = sorted(parquet_dir.glob("*.parquet"))
    check("parquet files present", len(files) == 5, str(files))

    stats = full_stats()
    check("row count 11,175,629", stats["total_logs"] == 11_175_629, str(stats["total_logs"]))
    check("unique blocks 575,061", stats["unique_blocks"] == 575_061, str(stats["unique_blocks"]))
    check("anomalous blocks 16,838", stats["actual_anomalous_blocks"] == 16_838, str(stats["actual_anomalous_blocks"]))
    check("detected_anomalies is null", stats["detected_anomalies"] is None, str(stats["detected_anomalies"]))
    check("model predictions unavailable", stats["model_predictions_available"] is False)

    page1 = fetch_logs(after_line=0, limit=50)
    recs1 = page1["records"]
    ids1 = [r["LineId"] for r in recs1]
    check("page 1 has 50 rows", len(recs1) == 50, str(len(recs1)))
    check("page 1 starts at line 1", ids1[0] == 1, str(ids1[:3]))
    check("page 1 ids unique", len(ids1) == len(set(ids1)))
    check("page 1 strictly increasing", ids1 == sorted(ids1) and ids1[-1] - ids1[0] == 49, str(ids1[:3] + ids1[-3:]))
    check("page 1 scores are null", all(r["anomaly_score"] is None and r["model_prediction"] is None for r in recs1))
    check("page 1 labels are ground truth", all(r["Label"] in {"Normal", "Anomaly"} for r in recs1))
    check("page 1 has more", page1["paging"]["has_more"] is True)

    page2 = fetch_logs(after_line=ids1[-1], limit=50)
    recs2 = page2["records"]
    ids2 = [r["LineId"] for r in recs2]
    check("page 2 has 50 rows", len(recs2) == 50, str(len(recs2)))
    check("no page overlap", set(ids1).isdisjoint(ids2), str(sorted(set(ids1) & set(ids2))[:5]))
    check("page 2 continues after page 1", ids2[0] == ids1[-1] + 1, f"{ids1[-1]} -> {ids2[0]}")
    check("page 2 strictly increasing", ids2 == sorted(ids2))

    late = fetch_logs(after_line=9_000_000, limit=5)
    late_ids = [r["LineId"] for r in late["records"]]
    check("late page after 9e6", late_ids and late_ids[0] > 9_000_000, str(late_ids[:3]))
    check("late page scores null", all(r["anomaly_score"] is None for r in late["records"]))

    first_block = recs1[0]["BlockId"]
    block = fetch_block(first_block, limit=50)
    check("block lookup found", block["found"] is True, first_block)
    check("block rows share BlockId", all(r["BlockId"] == first_block for r in block["records"]))
    check("block has ground-truth label", block["ground_truth_label"] in {"Normal", "Anomaly", "Mixed"})
    check("block scores unavailable", block["model_predictions_available"] is False)

    empty = fetch_logs(after_line=0, limit=10, block_id="blk_DOES_NOT_EXIST_xyz")
    check("unknown block empty page", empty["records"] == [])

    labelled = fetch_logs(after_line=0, limit=10, anomaly=1)
    check("anomaly filter returns labelled rows", labelled["records"] and all(r["Label"] == "Anomaly" for r in labelled["records"]))


def http_tests() -> None:
    status, health = get_json("/api/health")
    check("GET /api/health", status == 200 and health.get("status") == "ok", str(health))

    status, demo_stats = get_json("/api/stats")
    check("GET /api/stats 2000 rows", status == 200 and demo_stats.get("total_logs") == 2000, str(demo_stats))
    check("GET /api/stats actual 69", demo_stats.get("actual_anomalies") == 69, str(demo_stats.get("actual_anomalies")))
    check("GET /api/stats detected 7", demo_stats.get("detected_anomalies") == 7, str(demo_stats.get("detected_anomalies")))

    status, anoms = get_json("/api/anomalies?limit=5")
    check("GET /api/anomalies is a list", status == 200 and isinstance(anoms, list) and len(anoms) == 5)
    if isinstance(anoms, list) and anoms:
        check("2k anomalies have scores", anoms[0].get("anomaly_score") not in (None, ""))

    status, metrics = get_json("/api/hdfs-full-metrics")
    improved = metrics.get("improved_held_out_blocks") if isinstance(metrics, dict) else None
    check("GET /api/hdfs-full-metrics available", status == 200 and metrics.get("available") is True)
    if isinstance(improved, dict):
        check("held-out precision 0.3181", round(float(improved["precision"]), 4) == 0.3181, str(improved.get("precision")))
        check("held-out recall 0.5736", round(float(improved["recall"]), 4) == 0.5736, str(improved.get("recall")))
        check("held-out f1 0.4093", round(float(improved["f1"]), 4) == 0.4093, str(improved.get("f1")))
        check("held-out pr_auc 0.2370", round(float(improved["pr_auc"]), 4) == 0.2370, str(improved.get("pr_auc")))

    status, corpus = get_json("/api/hdfs-full/stats")
    check("GET /api/hdfs-full/stats rows", status == 200 and corpus.get("total_logs") == 11_175_629, str(corpus.get("total_logs")))
    check("GET /api/hdfs-full/stats blocks", corpus.get("unique_blocks") == 575_061)
    check("GET /api/hdfs-full/stats anomalous blocks", corpus.get("actual_anomalous_blocks") == 16_838)
    check("GET /api/hdfs-full/stats detections null", corpus.get("detected_anomalies") is None)

    status, logs = get_json("/api/hdfs-full/logs?after_line=0&limit=20")
    recs = logs.get("records") if isinstance(logs, dict) else None
    recs = recs if isinstance(recs, list) else []
    check("GET /api/hdfs-full/logs page", status == 200 and len(recs) == 20, str(len(recs) if recs else logs))
    if recs:
        check("HTTP logs start at line 1", recs[0]["LineId"] == 1, str(recs[0].get("LineId")))
        check("HTTP logs have content", bool(recs[0].get("Content")))
        check("HTTP logs scores null", recs[0].get("anomaly_score") is None)
        check("HTTP logs prediction null", recs[0].get("model_prediction") is None)

    next_after = recs[-1]["LineId"] if recs else 0
    status, page2 = get_json(f"/api/hdfs-full/logs?after_line={next_after}&limit=20")
    recs2 = page2.get("records") if isinstance(page2, dict) else None
    recs2 = recs2 if isinstance(recs2, list) else []
    ids1 = {r["LineId"] for r in recs}
    ids2 = {r["LineId"] for r in recs2}
    check("HTTP page 2 no overlap", status == 200 and ids1.isdisjoint(ids2), str(sorted(ids1 & ids2)[:5]))

    if recs:
        block_id = recs[0]["BlockId"]
        q = urllib.parse.urlencode({"block_id": block_id})
        status, block = get_json("/api/hdfs-full/blocks?" + q)
        check("GET /api/hdfs-full/blocks found", status == 200 and block.get("found") is True, str(status))
        check("block payload scores unavailable", block.get("model_predictions_available") is False)

    status, missing = get_json("/api/hdfs-full/blocks?block_id=blk_DOES_NOT_EXIST_xyz")
    check("missing block is 404", status == 404, str(status) + " " + str(missing))

    status, bad = get_json("/api/hdfs-full/logs?label=maybe")
    check("bad label is 400", status == 400, str(status) + " " + str(bad))

    status, empty = get_json("/api/hdfs-full/logs?q=__no_such_token_zzz__&limit=5")
    check("empty search returns zero rows", status == 200 and empty.get("records") == [], str(empty.get("returned")))


def main() -> int:
    print("== store ==")
    store_tests()
    print("== http ==")
    try:
        http_tests()
    except urllib.error.URLError as exc:
        FAILED.append("http server")
        print("FAIL  HTTP server unreachable: " + str(exc))
    print()
    if FAILED:
        print("FAILED: " + ", ".join(FAILED))
        return 1
    print("All browse checks passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
