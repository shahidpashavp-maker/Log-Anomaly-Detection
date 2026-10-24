"""Inspect HDFS log files in data/HDFS/. Does not modify the dataset."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "HDFS"
LABEL_HINTS = (
    "label",
    "anomaly",
    "anomal",
    "normal",
    "attack",
    "is_anomaly",
    "y",
    "ground",
    "class",
)


def print_section(title: str) -> None:
    print("\n" + "=" * 80)
    print(title)
    print("=" * 80)


def inspect_csv(path: Path) -> None:
    print_section(f"CSV: {path.name}")
    print(f"path: {path}")
    print(f"size_bytes: {path.stat().st_size}")
    df = pd.read_csv(path)
    print(f"rows: {len(df)}")
    print(f"columns: {len(df.columns)}")
    print("column_names:")
    for col in df.columns:
        print(f"  - {col} ({df[col].dtype})")
    print("\nfirst_10_rows:")
    print(df.head(10).to_string(index=False))
    print("\nmissing_value_counts:")
    print(df.isna().sum().to_string())
    print(f"\nduplicate_rows: {int(df.duplicated().sum())}")

    cat_cols = df.select_dtypes(include=["object", "string", "category"]).columns
    print("\ncategorical_unique_counts:")
    for col in cat_cols:
        nunique = df[col].nunique(dropna=True)
        print(f"  {col}: {nunique} unique")
        if nunique <= 30:
            print(df[col].value_counts(dropna=False).to_string())
        else:
            print(df[col].value_counts(dropna=False).head(10).to_string())
            print("  ... (showing top 10)")

    num_cols = df.select_dtypes(include=["number"]).columns
    if len(num_cols):
        print("\nnumerical_describe:")
        print(df[num_cols].describe().to_string())
    else:
        print("\nnumerical_describe: none")

    possible_labels = [
        col
        for col in df.columns
        if any(hint in str(col).lower() for hint in LABEL_HINTS)
    ]
    print("\npotential_label_columns_by_name:")
    if possible_labels:
        for col in possible_labels:
            print(f"  {col}")
            print(df[col].value_counts(dropna=False).to_string())
    else:
        print("  none")


def inspect_log(path: Path) -> None:
    print_section(f"LOG: {path.name}")
    print(f"path: {path}")
    print(f"size_bytes: {path.stat().st_size}")
    text = path.read_text(encoding="utf-8", errors="replace")
    lines = text.splitlines()
    print(f"line_count: {len(lines)}")
    print("first_10_lines:")
    for line in lines[:10]:
        print(line)
    print("\ninferred_structure:")
    print("  Typical HDFS raw line: Date Time Pid Level Component: Content")
    print("  Example: 081109 203615 148 INFO dfs.DataNode$PacketResponder: ...")


def main() -> None:
    print_section("Dataset location")
    print(f"data_dir: {DATA_DIR}")
    if not DATA_DIR.exists():
        print("ERROR: data/HDFS does not exist")
        return
    files = sorted(p for p in DATA_DIR.iterdir() if p.is_file() and p.name != ".gitkeep")
    print("files:")
    if not files:
        print("  none found")
        return
    for path in files:
        print(f"  {path.name} ({path.stat().st_size} bytes)")

    expected = [
        "HDFS_templates.csv",
        "anomaly_label.csv",
        "Event_traces.csv",
        "Event_occurrence_matrix.csv",
        "HDFS.npz",
    ]
    present = {p.name for p in files}
    print("\nexpected_full_HDFS_bundle:")
    for name in expected:
        print(f"  {name}: {'present' if name in present else 'MISSING'}")

    for path in files:
        suffix = path.suffix.lower()
        if suffix == ".csv":
            inspect_csv(path)
        elif suffix == ".log" or path.name.endswith(".log"):
            inspect_log(path)
        else:
            print_section(f"OTHER: {path.name}")
            print(f"size_bytes: {path.stat().st_size}")
            print("not parsed (non-csv/log)")


if __name__ == "__main__":
    main()
