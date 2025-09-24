from __future__ import annotations

import csv
import orjson
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Iterator, List, Optional, Tuple


@dataclass
class Pharmacy:
    id: str
    chain: str


@dataclass
class Claim:
    id: str
    npi: str
    ndc: str
    price: float
    quantity: float
    timestamp: str


@dataclass
class Revert:
    id: str
    claim_id: str
    timestamp: str


def iter_files(dirs_or_files: List[str], suffixes={".json", ".csv"}) -> Iterator[Path]:
    """
    Yield files under each provided path. Each path may be a file or a directory.
    """
    for pth in dirs_or_files:
        p = Path(pth)
        if not p.exists():
            continue
        if p.is_file() and p.suffix in suffixes:
            yield p
        elif p.is_dir():
            for fp in p.rglob("*"):
                if fp.suffix in suffixes and fp.is_file():
                    yield fp


def load_pharmacies(pharm_paths: List[str]) -> Dict[str, Pharmacy]:
    """
    Return mapping pharmacy_id (npi) -> Pharmacy. Accepts CSV or JSON (array or NDJSON).
    Supports headers: id, npi, pharmacy_id (case-insensitive).
    """
    result: Dict[str, Pharmacy] = {}
    for fp in iter_files(pharm_paths, suffixes={".csv", ".json"}):
        try:
            if fp.suffix == ".csv":
                with fp.open("r", newline="") as f:
                    reader = csv.DictReader(f)
                    for row in reader:
                        # normalize keys, tolerate id/npi/pharmacy_id
                        keys = {k.lower(): k for k in row.keys()}
                        id_key = None
                        for cand in ("id", "npi", "pharmacy_id"):
                            if cand in keys:
                                id_key = keys[cand]
                                break
                        if not id_key:
                            continue
                        pid = (row.get(id_key) or "").strip()
                        chain = (row.get(keys.get("chain", "chain"))
                                 or "").strip()
                        if pid:
                            result[pid] = Pharmacy(id=pid, chain=chain)
            else:
                # JSON: may be array or ndjson; accept id or npi for identifier
                with fp.open("rb") as f:
                    buf = f.read().strip()
                    if not buf:
                        continue

                    def upsert(obj):
                        if not isinstance(obj, dict):
                            return
                        # case-insensitive lookup
                        lower = {k.lower(): v for k, v in obj.items()}
                        pid = str(lower.get("id") or lower.get("npi")
                                  or lower.get("pharmacy_id") or "").strip()
                        chain = str(lower.get("chain") or "").strip()
                        if pid:
                            result[pid] = Pharmacy(id=pid, chain=chain)
                    if buf[:1] == b"[":
                        for row in orjson.loads(buf):
                            upsert(row)
                    else:
                        for line in buf.splitlines():
                            if line.strip():
                                try:
                                    upsert(orjson.loads(line))
                                except Exception:
                                    continue
        except Exception:
            continue
    return result


def _json_rows(fp: Path):
    with fp.open("rb") as f:
        data = f.read().strip()
        if not data:
            return
        if data[:1] == b"[":
            arr = orjson.loads(data)
            for obj in arr:
                if isinstance(obj, dict):
                    yield obj
        else:
            for line in data.splitlines():
                if not line.strip():
                    continue
                try:
                    obj = orjson.loads(line)
                    if isinstance(obj, dict):
                        yield obj
                except Exception:
                    continue


def load_claims(claim_paths: List[str], valid_npis: Optional[set[str]] = None):
    """
    Load claims, return (list_of_claims, index: claim_id -> (npi, ndc)).
    """
    claims: List[Claim] = []
    claim_index: Dict[str, Tuple[str, str]] = {}
    for fp in iter_files(claim_paths, suffixes={".json"}):
        try:
            for row in _json_rows(fp):
                id_ = str(row.get("id") or "").strip()
                npi = str(row.get("npi") or "").strip()
                ndc = str(row.get("ndc") or "").strip()
                try:
                    price = float(row.get("price"))
                    quantity = float(row.get("quantity"))
                except Exception:
                    continue
                ts = str(row.get("timestamp") or "").strip()
                if not (id_ and npi and ndc):
                    continue
                if valid_npis is not None and npi not in valid_npis:
                    continue
                claims.append(Claim(id=id_, npi=npi, ndc=ndc,
                              price=price, quantity=quantity, timestamp=ts))
                claim_index[id_] = (npi, ndc)
        except Exception:
            continue
    return claims, claim_index


def load_reverts(revert_paths: List[str]) -> List[Revert]:
    reverts: List[Revert] = []
    for fp in iter_files(revert_paths, suffixes={".json"}):
        try:
            for row in _json_rows(fp):
                id_ = str(row.get("id") or "").strip()
                claim_id = str(row.get("claim_id") or "").strip()
                ts = str(row.get("timestamp") or "").strip()
                if id_ and claim_id:
                    reverts.append(
                        Revert(id=id_, claim_id=claim_id, timestamp=ts))
        except Exception:
            continue
    return reverts


def load_inputs(pharmacies_path_or_dir: str, claims_dir: str, reverts_dir: str):
    """
    Convenience loader that reads:
      - pharmacies from a file OR directory
      - all claims JSON from claims_dir (recursively)
      - all reverts JSON from reverts_dir (recursively)
    Returns: (pharmacies_dict, claims_list, reverts_list)
    """
    pharmacies = load_pharmacies([pharmacies_path_or_dir])
    valid_npis = set(pharmacies.keys())
    claims, _ = load_claims([claims_dir], valid_npis=valid_npis)
    reverts = load_reverts([reverts_dir])
    return pharmacies, claims, reverts
