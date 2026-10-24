from __future__ import annotations

import json

from anomaly_pipeline import METRICS_PATH, train_and_evaluate


def _print_metrics(title: str, metrics: dict) -> None:
    print(f"\n=== {title} ===")
    for key in (
        "scope",
        "protocol",
        "n",
        "actual_anomalies",
        "detected_anomalies",
        "precision",
        "recall",
        "f1",
        "false_positive_rate",
        "tp",
        "tn",
        "fp",
        "fn",
        "processing_time_sec",
    ):
        if key not in metrics:
            continue
        value = metrics[key]
        if isinstance(value, float):
            print(f"{key}: {value:.4f}")
        else:
            print(f"{key}: {value}")


if __name__ == "__main__":
    df, metrics = train_and_evaluate()
    report = json.loads(METRICS_PATH.read_text(encoding="utf-8"))

    print("=== Isolation Forest Training Complete ===")
    _print_metrics("Baseline (original protocol, in-sample lines)", report["baseline_in_sample"])
    _print_metrics(
        "Baseline (original features, held-out blocks)",
        report["baseline_held_out_blocks"],
    )
    _print_metrics("Improved validation blocks", report["improved_validation_blocks"])
    _print_metrics("Improved held-out blocks (primary)", report["improved_held_out_blocks"])
    _print_metrics("Improved held-out lines", report["improved_held_out_lines"])

    print("\n=== Selected settings ===")
    settings = report["settings"]
    print(f"train_on: {settings['train_on']}")
    print(f"n_estimators: {settings['n_estimators']}")
    print(f"max_samples: {settings['max_samples']}")
    print(f"contamination: {settings['contamination']}")
    print(f"decision_threshold: {settings['decision_threshold']:.6f}")
    print(f"split: {settings['split']}")
    print(f"processing_time_sec: {report['processing_time_sec']:.4f}")

    print("\n=== Limitations ===")
    for item in report["limitations"]:
        print(f"- {item}")

    print("\nSaved model: artifacts/isolation_forest_pipeline.joblib")
    print("Saved results: results/anomaly_results.csv")
    print("Saved metrics: results/metrics.json")
    print("\nTop detected anomalies:")
    cols = ["LineId", "BlockId", "EventId", "Level", "Component", "split", "anomaly_score"]
    print(df.sort_values("anomaly_score", ascending=False)[cols].head(10).to_string(index=False))

    print("\nAPI summary keys:")
    for key, value in metrics.items():
        if isinstance(value, float):
            print(f"{key}: {value:.4f}")
        else:
            print(f"{key}: {value}")
