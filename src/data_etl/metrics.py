from __future__ import annotations

from collections import defaultdict
from typing import Iterable, List, Optional, Set

from .utils import Claim, Revert


def compute_metrics(
    claims: Iterable[Claim],
    reverts: Iterable[Revert],
    valid_npis: Optional[Set[str]] = None,
) -> List[dict]:
    """
    Output per (npi, ndc):
      - fills: count of all claims
      - reverted: count of reverts linked to those claims
      - avg_price: average unit price from NON-reverted claims (price/quantity)
      - total_price: sum of price from NON-reverted claims
    """
    reverted_ids = {r.claim_id for r in reverts}

    groups = defaultdict(lambda: {
        "fills": 0,
        "reverted": 0,
        "sum_unit_price": 0.0,
        "sum_price": 0.0,
        "n_non_reverted": 0,
    })

    for c in claims:
        # if c.ndc == "00093752910" and c.npi == "3333333333":
            if valid_npis is not None and c.npi not in valid_npis:
                continue

            key = (c.npi, c.ndc)
            g = groups[key]
            g["fills"] += 1

            if c.id in reverted_ids:
                g["reverted"] += 1
            else:
                if c.quantity and c.quantity > 0:
                    unit = c.price / c.quantity
                    g["sum_unit_price"] += unit
                    g["sum_price"] += c.price
                    g["n_non_reverted"] += 1
                else:
                    g["sum_price"] += c.price

    output: List[dict] = []
    for (npi, ndc), g in groups.items():
        n = g["n_non_reverted"]
        avg_price = (g["sum_unit_price"] / n) if n else 0.0
        output.append({
            "npi": npi,
            "ndc": ndc,
            "fills": g["fills"],
            "reverted": g["reverted"],
            "avg_price": round(avg_price, 2),
            "total_price": round(g["sum_price"], 2),
        })

    output.sort(key=lambda r: (r["npi"], r["ndc"]))
    return output
