"""Pure-Python reference implementation used to verify business logic locally.

This is NOT an implementation of Spark, Auto Loader, Delta Lake, or Unity Catalog.
The Databricks notebooks contain the corresponding cloud implementation.
"""
from __future__ import annotations
from collections import defaultdict
from datetime import datetime


def validate(event: dict) -> str | None:
    if not event.get("event_id") or not event.get("rx_id"):
        return "missing_identifiers"
    if event.get("operation") not in {"I", "U", "D"}:
        return "unsupported_operation"
    if not event.get("event_ts") or not isinstance(event.get("event_seq"), int):
        return "missing_event_order"
    try:
        datetime.fromisoformat(event["event_ts"])
    except (ValueError, TypeError):
        return "invalid_timestamp"
    if event["operation"] != "D":
        if not event.get("hcp_id") or not event.get("product_id"):
            return "missing_dimension_key"
        if not isinstance(event.get("quantity"), int) or event["quantity"] <= 0:
            return "invalid_quantity"
    return None


def materialize(events: list[dict], hcps: list[dict], products: list[dict]) -> dict:
    """Compute expected current state, history and gold-level metrics."""
    unique: dict[str, dict] = {}
    duplicate_count = 0
    for event in events:
        eid = event.get("event_id")
        if eid in unique:
            duplicate_count += 1
            continue
        unique[eid] = event
    valid, rejected = [], []
    for event in unique.values():
        error = validate(event)
        if error:
            rejected.append({"event_id": event.get("event_id"), "reason": error})
        else:
            valid.append(event)
    by_rx: dict[str, dict] = {}
    for event in valid:
        k = event["rx_id"]
        ordering = lambda r: (r["event_seq"], r["event_ts"], r["event_id"])
        if k not in by_rx or ordering(event) > ordering(by_rx[k]):
            by_rx[k] = event
    live = [e for e in by_rx.values() if e["operation"] != "D"]
    hcp_keys = {h["hcp_id"] for h in hcps}
    product_keys = {p["product_id"] for p in products}
    for event in live:
        if event["hcp_id"] not in hcp_keys or event["product_id"] not in product_keys:
            raise ValueError("Unknown dimensional key in synthetic facts")
    metrics: dict[tuple, dict] = defaultdict(lambda: {"rx_count": 0, "units": 0})
    for event in live:
        k = (event["event_ts"][:10], event["region"], event["product_id"])
        metrics[k]["rx_count"] += 1
        metrics[k]["units"] += event["quantity"]
    marts = [
        {"rx_date": day, "region": region, "product_id": product,
         "rx_count": result["rx_count"], "units": result["units"]}
        for (day, region, product), result in sorted(metrics.items())
    ]
    return {"current": sorted(by_rx.values(), key=lambda r: r["rx_id"]),
            "active": sorted(live, key=lambda r: r["rx_id"]),
            "history": sorted(valid, key=lambda r: (r["rx_id"], r["event_seq"], r["event_ts"])),
            "quarantine": rejected, "duplicate_events": duplicate_count,
            "daily_metrics": marts}
