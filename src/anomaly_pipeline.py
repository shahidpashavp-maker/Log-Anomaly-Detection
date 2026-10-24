from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple

import joblib
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import IsolationForest
from sklearn.metrics import confusion_matrix, f1_score, precision_score, recall_score
from sklearn.model_selection import train_test_split
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler

ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data" / "HDFS"
ARTIFACT_DIR = ROOT / "artifacts"
MODEL_PATH = ARTIFACT_DIR / "isolation_forest_pipeline.joblib"
RESULTS_PATH = ROOT / "results" / "anomaly_results.csv"
METRICS_PATH = ROOT / "results" / "metrics.json"

BLOCK_RE = re.compile(r"blk_-?\d+")
IP_RE = re.compile(r"\b(?:\d{1,3}\.){3}\d{1,3}(?::\d+)?\b")

CSV_STRING_COLS = {"Date": str, "Time": str, "Pid": str}

CATEGORICAL = ["Level", "Component", "EventId"]
NUMERIC = [
    "message_length",
    "token_count",
    "digit_count",
    "ip_count",
    "has_exception",
    "is_delete",
    "is_replicate",
    "is_verify",
    "is_served",
    "is_warn",
    "block_mentions",
    "hour",
    "n_lines",
    "n_events",
    "event_frequency",
    "component_frequency",
    "event_component_frequency",
]
FEATURES = CATEGORICAL + NUMERIC

BASELINE_CATEGORICAL = ["Level", "Component", "EventId"]
BASELINE_NUMERIC = [
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
]
BASELINE_FEATURES = BASELINE_CATEGORICAL + BASELINE_NUMERIC

SPLIT_RANDOM_STATE = 42
TRAIN_FRAC = 0.60
VAL_FRAC = 0.20
TEST_FRAC = 0.20


def extract_block_id(content: pd.Series) -> pd.Series:
    """Use the first blk_<id> in each line. HDFS_2k always has at least one."""
    return content.fillna("").astype(str).str.extract(r"(blk_-?\d+)", expand=False)


def _pair_key(event_id: pd.Series, component: pd.Series) -> pd.Series:
    return event_id.astype(str) + "|" + component.astype(str)


def add_line_features(df: pd.DataFrame) -> pd.DataFrame:
    """Lexical / semantic features derived only from the current rows."""
    out = df.copy()
    content = out["Content"].fillna("").astype(str)
    low = content.str.lower()
    out["message_length"] = content.str.len()
    out["token_count"] = content.str.split().str.len()
    out["digit_count"] = content.str.count(r"\d")
    out["ip_count"] = content.apply(lambda x: len(IP_RE.findall(x)))
    out["has_exception"] = low.str.contains(
        r"exception|error|failed|interrupted", regex=True
    ).astype(int)
    out["has_warning_word"] = low.str.contains(r"warn|warning", regex=True).astype(int)
    out["is_delete"] = low.str.contains(r"delete|invalidset", regex=True).astype(int)
    out["is_replicate"] = low.str.contains(r"replicate|transfer", regex=True).astype(int)
    out["is_verify"] = low.str.contains(r"verification", regex=True).astype(int)
    out["is_served"] = low.str.contains(r"served block|serving", regex=True).astype(int)
    out["is_warn"] = (out["Level"].astype(str) == "WARN").astype(int)
    out["block_mentions"] = content.apply(lambda x: len(BLOCK_RE.findall(x)))
    out["hour"] = out["Time"].astype(str).str.zfill(6).str[:2].astype(int)
    return out


def frequency_maps(ref: pd.DataFrame) -> Dict[str, pd.Series]:
    pair = _pair_key(ref["EventId"], ref["Component"])
    return {
        "event": ref["EventId"].value_counts(normalize=True),
        "component": ref["Component"].value_counts(normalize=True),
        "pair": pair.value_counts(normalize=True),
    }


def apply_frequency_maps(df: pd.DataFrame, maps: Dict[str, pd.Series]) -> pd.DataFrame:
    out = df.copy()
    pair = _pair_key(out["EventId"], out["Component"])
    out["event_frequency"] = out["EventId"].map(maps["event"]).fillna(0.0).astype(float)
    out["component_frequency"] = out["Component"].map(maps["component"]).fillna(0.0).astype(float)
    out["event_component_frequency"] = pair.map(maps["pair"]).fillna(0.0).astype(float)
    return out


def load_raw() -> pd.DataFrame:
    logs = pd.read_csv(DATA_DIR / "HDFS_2k.log_structured.csv", dtype=CSV_STRING_COLS)
    labels = pd.read_csv(DATA_DIR / "anomaly_label.csv")
    logs["BlockId"] = extract_block_id(logs["Content"])
    logs = logs.merge(labels, on="BlockId", how="left")
    logs["Label"] = logs["Label"].fillna("Unknown")
    return logs


def load_and_prepare() -> pd.DataFrame:
    """Load HDFS_2k, join block labels, and add line features.

    Frequency columns here are computed on the full file for dashboard fallback
    only. Training uses train-only maps inside train_and_evaluate().
    """
    logs = add_line_features(load_raw())
    return apply_frequency_maps(logs, frequency_maps(logs))


def _mode_or_first(series: pd.Series) -> Any:
    modes = series.mode(dropna=True)
    if len(modes):
        return modes.iloc[0]
    return series.iloc[0] if len(series) else np.nan


def lines_to_blocks(line_df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate log lines to the HDFS evaluation unit (BlockId)."""
    grouped = line_df.groupby("BlockId", dropna=False)
    blocks = grouped.agg(
        Label=("Label", _mode_or_first),
        Level=("Level", _mode_or_first),
        Component=("Component", _mode_or_first),
        EventId=("EventId", _mode_or_first),
        message_length=("message_length", "mean"),
        token_count=("token_count", "mean"),
        digit_count=("digit_count", "mean"),
        ip_count=("ip_count", "mean"),
        hour=("hour", "mean"),
        has_exception=("has_exception", "max"),
        has_warning_word=("has_warning_word", "max"),
        is_delete=("is_delete", "max"),
        is_replicate=("is_replicate", "max"),
        is_verify=("is_verify", "max"),
        is_served=("is_served", "max"),
        is_warn=("is_warn", "max"),
        block_mentions=("block_mentions", "max"),
        n_lines=("LineId", "count"),
        n_events=("EventId", "nunique"),
    ).reset_index()
    return blocks


def classification_metrics(y_true: Iterable[int], y_pred: Iterable[int]) -> Dict[str, Any]:
    y_true = np.asarray(y_true, dtype=int)
    y_pred = np.asarray(y_pred, dtype=int)
    tn, fp, fn, tp = confusion_matrix(y_true, y_pred, labels=[0, 1]).ravel()
    return {
        "n": int(len(y_true)),
        "actual_anomalies": int(y_true.sum()),
        "detected_anomalies": int(y_pred.sum()),
        "precision": float(precision_score(y_true, y_pred, zero_division=0)),
        "recall": float(recall_score(y_true, y_pred, zero_division=0)),
        "f1": float(f1_score(y_true, y_pred, zero_division=0)),
        "false_positive_rate": float(fp / (fp + tn)) if (fp + tn) else 0.0,
        "tp": int(tp),
        "tn": int(tn),
        "fp": int(fp),
        "fn": int(fn),
    }


def split_block_ids(
    blocks: pd.DataFrame, random_state: int = SPLIT_RANDOM_STATE
) -> Tuple[np.ndarray, np.ndarray, np.ndarray]:
    labeled = blocks[blocks["Label"].isin(["Anomaly", "Normal"])].copy()
    y = (labeled["Label"] == "Anomaly").astype(int)
    ids = labeled["BlockId"]
    id_train, id_rest, y_train, y_rest = train_test_split(
        ids, y, test_size=(1.0 - TRAIN_FRAC), random_state=random_state, stratify=y
    )
    relative_test = TEST_FRAC / (VAL_FRAC + TEST_FRAC)
    id_val, id_test, _, _ = train_test_split(
        id_rest, y_rest, test_size=relative_test, random_state=random_state, stratify=y_rest
    )
    return id_train.to_numpy(), id_val.to_numpy(), id_test.to_numpy()


def build_pipeline(
    categorical: List[str] = CATEGORICAL,
    numeric: List[str] = NUMERIC,
    n_estimators: int = 200,
    contamination: float | str = "auto",
    max_samples: int | str = "auto",
    random_state: int = 42,
) -> Pipeline:
    pre = ColumnTransformer(
        [
            ("cat", OneHotEncoder(handle_unknown="ignore", sparse_output=False), categorical),
            ("num", StandardScaler(), numeric),
        ],
        remainder="drop",
    )
    model = IsolationForest(
        n_estimators=n_estimators,
        contamination=contamination,
        max_samples=max_samples,
        random_state=random_state,
        n_jobs=-1,
    )
    return Pipeline([("preprocess", pre), ("model", model)])


def anomaly_scores(pipeline: Pipeline, frame: pd.DataFrame, features: List[str]) -> np.ndarray:
    return -pipeline.decision_function(frame[features])


def tune_threshold(scores: np.ndarray, y_true: np.ndarray) -> Tuple[float, Dict[str, Any]]:
    """Pick the score cutoff that maximizes validation F1, then precision."""
    y_true = np.asarray(y_true, dtype=int)
    scores = np.asarray(scores, dtype=float)
    best: Dict[str, Any] | None = None
    best_threshold = float(np.quantile(scores, 0.95)) if len(scores) else 0.0

    candidates = np.unique(scores)
    if not len(candidates):
        return best_threshold, classification_metrics(y_true, np.zeros_like(y_true))

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
    return best_threshold, best


def _select_rows(df: pd.DataFrame, block_ids: Iterable[Any]) -> pd.DataFrame:
    return df[df["BlockId"].isin(set(block_ids))].copy()


def _search_improved_model(
    train_blocks: pd.DataFrame, val_blocks: pd.DataFrame
) -> Dict[str, Any]:
    """Small leakage-safe grid. Fit on train, choose by validation F1."""
    search = []
    for train_on in ("normal", "all"):
        for n_estimators in (200, 400):
            for max_samples in ("auto", 256):
                fit_df = train_blocks[train_blocks["y"] == 0] if train_on == "normal" else train_blocks
                if fit_df.empty:
                    continue
                pipeline = build_pipeline(
                    n_estimators=n_estimators,
                    contamination="auto",
                    max_samples=max_samples,
                )
                pipeline.fit(fit_df[FEATURES])
                val_scores = anomaly_scores(pipeline, val_blocks, FEATURES)
                threshold, val_metrics = tune_threshold(val_scores, val_blocks["y"].to_numpy())
                search.append(
                    {
                        "train_on": train_on,
                        "n_estimators": n_estimators,
                        "max_samples": max_samples,
                        "contamination": "auto",
                        "threshold": threshold,
                        "val_metrics": val_metrics,
                        "pipeline": pipeline,
                    }
                )

    search.sort(
        key=lambda item: (
            item["val_metrics"]["f1"],
            item["val_metrics"]["precision"],
            -item["val_metrics"]["false_positive_rate"],
        ),
        reverse=True,
    )
    return search[0]


def _evaluate_baseline_held_out(
    train_blocks: pd.DataFrame, test_blocks: pd.DataFrame
) -> Dict[str, Any]:
    """Original IF settings, but fit only on train and scored on held-out blocks."""
    pipeline = build_pipeline(
        categorical=BASELINE_CATEGORICAL,
        numeric=BASELINE_NUMERIC,
        n_estimators=200,
        contamination=0.05,
        max_samples="auto",
    )
    pipeline.fit(train_blocks[BASELINE_FEATURES])
    pred = (pipeline.predict(test_blocks[BASELINE_FEATURES]) == -1).astype(int)
    metrics = classification_metrics(test_blocks["y"].to_numpy(), pred)
    metrics["scope"] = "held_out_test_blocks"
    metrics["protocol"] = "original_features_contamination_0.05_fit_train"
    return metrics


def _evaluate_baseline_in_sample(lines: pd.DataFrame) -> Tuple[Dict[str, Any], float]:
    """Original protocol: frequencies + fit + score on every log line."""
    started = time.perf_counter()
    frame = apply_frequency_maps(lines.copy(), frequency_maps(lines))
    pipeline = build_pipeline(
        categorical=BASELINE_CATEGORICAL,
        numeric=BASELINE_NUMERIC,
        n_estimators=200,
        contamination=0.05,
        max_samples="auto",
    )
    pipeline.fit(frame[BASELINE_FEATURES])
    pred = (pipeline.predict(frame[BASELINE_FEATURES]) == -1).astype(int)
    y_true = (frame["Label"] == "Anomaly").astype(int)
    metrics = classification_metrics(y_true, pred)
    metrics["scope"] = "in_sample_all_lines"
    metrics["protocol"] = "original_features_contamination_0.05_fit_all_lines"
    metrics["processing_time_sec"] = float(time.perf_counter() - started)
    return metrics, metrics["processing_time_sec"]


def train_and_evaluate() -> Tuple[pd.DataFrame, dict]:
    ARTIFACT_DIR.mkdir(exist_ok=True)
    RESULTS_PATH.parent.mkdir(exist_ok=True)

    started = time.perf_counter()
    lines = add_line_features(load_raw())
    baseline_in_sample, _ = _evaluate_baseline_in_sample(lines)
    blocks = lines_to_blocks(lines)
    blocks = blocks[blocks["Label"].isin(["Anomaly", "Normal"])].copy()
    blocks["y"] = (blocks["Label"] == "Anomaly").astype(int)

    train_ids, val_ids, test_ids = split_block_ids(blocks)
    train_blocks = _select_rows(blocks, train_ids)
    val_blocks = _select_rows(blocks, val_ids)
    test_blocks = _select_rows(blocks, test_ids)

    maps = frequency_maps(train_blocks)
    train_blocks = apply_frequency_maps(train_blocks, maps)
    val_blocks = apply_frequency_maps(val_blocks, maps)
    test_blocks = apply_frequency_maps(test_blocks, maps)

    baseline_held_out = _evaluate_baseline_held_out(train_blocks, test_blocks)
    selected = _search_improved_model(train_blocks, val_blocks)
    pipeline = selected["pipeline"]
    threshold = selected["threshold"]

    test_scores = anomaly_scores(pipeline, test_blocks, FEATURES)
    test_pred = (test_scores >= threshold).astype(int)
    block_test = classification_metrics(test_blocks["y"].to_numpy(), test_pred)

    test_lines = apply_frequency_maps(_select_rows(lines, test_ids), maps)
    test_line_pred = test_lines["BlockId"].map(
        dict(zip(test_blocks["BlockId"], test_pred))
    ).fillna(0).astype(int)
    test_line_true = (test_lines["Label"] == "Anomaly").astype(int)
    line_test = classification_metrics(test_line_true, test_line_pred)

    all_blocks = apply_frequency_maps(blocks.copy(), maps)
    all_scores = anomaly_scores(pipeline, all_blocks, FEATURES)
    all_pred = (all_scores >= threshold).astype(int)
    block_lookup = pd.DataFrame(
        {
            "BlockId": all_blocks["BlockId"],
            "anomaly_prediction": all_pred,
            "anomaly_score": all_scores,
            "split": np.select(
                [
                    all_blocks["BlockId"].isin(train_ids),
                    all_blocks["BlockId"].isin(val_ids),
                    all_blocks["BlockId"].isin(test_ids),
                ],
                ["train", "validation", "test"],
                default="unused",
            ),
        }
    )

    result_lines = apply_frequency_maps(lines, maps)
    result_lines = result_lines.merge(block_lookup, on="BlockId", how="left")
    result_lines["anomaly_prediction"] = result_lines["anomaly_prediction"].fillna(0).astype(int)
    result_lines["anomaly_score"] = result_lines["anomaly_score"].fillna(0.0)
    result_lines["split"] = result_lines["split"].fillna("unused")

    elapsed = time.perf_counter() - started

    settings = {
        "model": "IsolationForest",
        "train_on": selected["train_on"],
        "n_estimators": selected["n_estimators"],
        "max_samples": selected["max_samples"],
        "contamination": selected["contamination"],
        "decision_threshold": threshold,
        "threshold_objective": "max validation F1, then precision",
        "split": {
            "unit": "BlockId",
            "stratified_by": "block Label",
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
        },
        "features": FEATURES,
        "frequency_maps_fit_on": "train blocks only",
        "labels_used_for": [
            "excluding known anomalies from IF training when train_on=normal",
            "validation threshold selection",
            "held-out evaluation",
        ],
        "evaluation_unit": "BlockId (HDFS labels are block-level)",
    }

    bundle = {
        "pipeline": pipeline,
        "threshold": threshold,
        "features": FEATURES,
        "categorical": CATEGORICAL,
        "numeric": NUMERIC,
        "frequency_maps": maps,
        "settings": settings,
    }

    report = {
        "baseline_in_sample": baseline_in_sample,
        "baseline_held_out_blocks": baseline_held_out,
        "improved_validation_blocks": selected["val_metrics"],
        "improved_held_out_blocks": block_test,
        "improved_held_out_lines": line_test,
        "settings": settings,
        "processing_time_sec": float(elapsed),
        "limitations": [
            "HDFS_2k has ~1 log line per block, so sequence-based HDFS signals are missing.",
            "Most anomalous lines use the same common templates as normal lines.",
            "Isolation Forest can mostly isolate rare templates, not semantic block failures.",
            "Validation/test anomaly counts are small (about 13-14 blocks), so metrics have high variance.",
            "Multi-block delete lines keep only the first BlockId.",
        ],
    }
    METRICS_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

    joblib.dump(bundle, MODEL_PATH)
    result_lines.to_csv(RESULTS_PATH, index=False)

    # Backward-compatible /api/retrain keys: honest held-out block metrics.
    metrics = {
        "total_logs": int(len(result_lines)),
        "actual_anomalies": int((result_lines["Label"] == "Anomaly").sum()),
        "detected_anomalies": int(result_lines["anomaly_prediction"].sum()),
        "precision": block_test["precision"],
        "recall": block_test["recall"],
        "f1": block_test["f1"],
        "false_positive_rate": block_test["false_positive_rate"],
        "tp": block_test["tp"],
        "tn": block_test["tn"],
        "fp": block_test["fp"],
        "fn": block_test["fn"],
        "evaluation": "held_out_test_blocks",
        "test_blocks": block_test["n"],
        "test_actual_anomalies": block_test["actual_anomalies"],
        "test_detected_anomalies": block_test["detected_anomalies"],
        "processing_time_sec": float(elapsed),
        "baseline_in_sample_f1": baseline_in_sample["f1"],
        "baseline_held_out_f1": baseline_held_out["f1"],
        "improved_held_out_f1": block_test["f1"],
        "decision_threshold": threshold,
        "train_on": selected["train_on"],
        "n_estimators": selected["n_estimators"],
        "max_samples": selected["max_samples"],
    }
    return result_lines, metrics


def load_bundle() -> Dict[str, Any]:
    if not MODEL_PATH.exists():
        train_and_evaluate()
    obj = joblib.load(MODEL_PATH)
    if isinstance(obj, dict) and "pipeline" in obj:
        return obj
    return {
        "pipeline": obj,
        "threshold": None,
        "features": FEATURES,
        "categorical": CATEGORICAL,
        "numeric": NUMERIC,
        "frequency_maps": None,
        "settings": {},
    }


def load_model() -> Pipeline:
    bundle = load_bundle()
    return bundle["pipeline"]
