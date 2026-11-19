"""Local, dependency-free proof of the synthetic data & CDC business rules."""
from __future__ import annotations
import argparse
import csv
import json
from pathlib import Path
from .synthetic import generate
from .reference import materialize


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, sort_keys=True) + "\n")


def _write_csv(path: Path, records: list[dict], keys: list[str]) -> None:
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=keys)
        writer.writeheader()
        writer.writerows({k: row.get(k, "") for k in keys} for row in records)


def main() -> None:
    p = argparse.ArgumentParser(description="Run local reference data pipeline")
    p.add_argument("--output", default="demo_outputs", help="Output directory")
    p.add_argument("--records", type=int, default=1000)
    args = p.parse_args()
    output = Path(args.output)
    source = output / "source"
    results = output / "results"
    source.mkdir(parents=True, exist_ok=True)
    results.mkdir(parents=True, exist_ok=True)
    dataset = generate(n=args.records)
    for key, records in dataset.items():
        _write_jsonl(source / f"{key}.jsonl", records)
    result = materialize(dataset["batch_1"] + dataset["batch_2"],
                         dataset["hcps"], dataset["products"])
    _write_csv(results / "fact_prescriptions_preview.csv", result["active"][:12],
               ["rx_id", "hcp_id", "product_id", "region", "quantity", "event_ts", "event_seq"])
    _write_csv(results / "daily_prescription_metrics.csv", result["daily_metrics"],
               ["rx_date", "region", "product_id", "rx_count", "units"])
    report = {
        "synthetic_input_events": len(dataset["batch_1"]) + len(dataset["batch_2"]),
        "duplicate_events_dropped": result["duplicate_events"],
        "quarantined_records": len(result["quarantine"]),
        "history_events_valid": len(result["history"]),
        "current_records_including_tombstones": len(result["current"]),
        "active_prescriptions": len(result["active"]),
        "deleted_prescriptions": sum(x["operation"] == "D" for x in result["current"]),
        "total_active_units": sum(x["quantity"] for x in result["active"]),
        "data_is_synthetic": True,
        "execution_mode": "stdlib reference (not actual Databricks)"
    }
    (results / "quality_report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Results written to: {output.resolve()}")


if __name__ == "__main__":
    main()
