"""Deterministic, non-identifying HCP and prescription event generator."""
from __future__ import annotations
from datetime import datetime, timedelta, timezone
from random import Random


def generate(n: int = 1000, seed: int = 21) -> dict:
    """Return independent source batches, dimensions, and deliberately bad events.

    An inserted row is followed by possible updates/deletes. The second batch
    includes two exact event replays, one late old-state event, and one bad row.
    This lets the demo test deduplication, CDC ordering, tombstones and quarantine.
    """
    if n < 20:
        raise ValueError("n must be at least 20")
    rng = Random(seed)
    regions = ("US", "EU", "APAC")
    products = [
        {"product_id": f"PRD{i:03d}", "product_name": f"Demo Product {i:03d}",
         "therapeutic_area": ("Oncology", "Neurology", "Immunology")[i % 3]}
        for i in range(1, 13)
    ]
    hcps = [
        {"hcp_id": f"HCP{i:04d}", "hcp_name": f"Synthetic HCP {i:04d}",
         "region": regions[(i - 1) % len(regions)],
         "specialty": ("General Medicine", "Oncology", "Neurology")[i % 3]}
        for i in range(1, 81)
    ]
    base = datetime(2026, 1, 1, 0, 0, tzinfo=timezone.utc)
    batch1: list[dict] = []
    for i in range(n):
        hcp = hcps[rng.randrange(len(hcps))]
        product = products[rng.randrange(len(products))]
        batch1.append({
            "event_id": f"EVT-I-{i+1:06d}", "rx_id": f"RX{i+1:06d}",
            "hcp_id": hcp["hcp_id"], "product_id": product["product_id"],
            "region": hcp["region"], "quantity": rng.randint(1, 5),
            "event_ts": (base + timedelta(minutes=i)).isoformat(),
            "event_seq": 1, "operation": "I", "source_system": "synthetic_crm"
        })
    batch2: list[dict] = []
    for i, orig in enumerate(batch1[:min(n, 120)]):
        batch2.append({**orig, "event_id": f"EVT-U-{i+1:06d}",
                       "event_ts": (base + timedelta(days=1, minutes=i)).isoformat(),
                       "quantity": orig["quantity"] + 2, "event_seq": 2, "operation": "U"})
    for i, orig in enumerate(batch1[:min(n, 20)]):
        batch2.append({**orig, "event_id": f"EVT-D-{i+1:06d}",
                       "event_ts": (base + timedelta(days=2, minutes=i)).isoformat(),
                       "quantity": 0, "event_seq": 3, "operation": "D"})
    batch2.extend([dict(batch1[3]), dict(batch2[0])])  # replayed event IDs
    batch2.append({**batch1[0], "event_id": "EVT-LATE-000001",
                   "event_ts": (base + timedelta(minutes=1)).isoformat(),
                   "quantity": 999, "event_seq": 1, "operation": "U"})
    batch2.append({**batch1[0], "event_id": "EVT-BAD-000001",
                   "rx_id": "RX-BAD", "product_id": "",
                   "quantity": -5, "event_seq": 1, "operation": "I"})
    return {"batch_1": batch1, "batch_2": batch2, "hcps": hcps,
            "products": products}
