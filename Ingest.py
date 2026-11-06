"""Tick sightings ingestion pipeline.

Fetches sightings from Elanco API, validates and cleans them, merges
duplicates, and stores them in ticks.db.

    pip install requests pydantic

    python Ingest.py                  # sync from the API
    python Ingest.py --self-test      # run the built-in tests
    python Ingest.py --benchmark 200000   # time a load of N fake records
    The testing and benchmarks here were written by claude to validate how the system deals with large datasets

Design (see README for the longer version)
------------------------------------------
raw_sightings  every distinct payload ever received, kept once
sightings      cleaned, one row per id; duplicates are merged
quarantine     records that are unusable (does not contain non-optional data) or that conflict with stored data
sync_runs      one row per sync: counts, status and any errors that may have occurred

Scale:
  * records are processed in chunks, one transaction per chunk, with batched
    lookups (IN queries) and executemany
  * every payload is hashed; payloads we've already seen are skipped, so a
    repeat sync of unchanged data takes very little resources
  * WAL mode, indexes on the filter columns, and an expression index for
    month grouping
"""
import argparse
import hashlib
import json
import random
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from pydantic import BaseModel, ValidationError, field_validator
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://dev-task.elancoapps.com/data"
DB_PATH = Path(__file__).parent / "ticks.db"

CHUNK_SIZE = 500  # number of records processed at a time
PLACEHOLDER = "Incomplete" # what is input if a record is incomplete
MERGE_FIELDS = ("city", "species", "latin_name")

SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_sightings (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT,
  id TEXT,
  content_hash TEXT NOT NULL UNIQUE,
  payload TEXT NOT NULL,
  fetched_at TEXT NOT NULL);
CREATE INDEX IF NOT EXISTS ix_raw_id ON raw_sightings(id);

CREATE TABLE IF NOT EXISTS sightings (
  id TEXT PRIMARY KEY,
  observed_at TEXT NOT NULL,          -- ISO 8601 text so it sorts/filters properly
  city TEXT NOT NULL,
  species TEXT NOT NULL,
  latin_name TEXT NOT NULL,
  completeness INTEGER NOT NULL);
CREATE INDEX IF NOT EXISTS ix_sightings_date ON sightings(observed_at);
CREATE INDEX IF NOT EXISTS ix_sightings_city ON sightings(city, observed_at);
CREATE INDEX IF NOT EXISTS ix_sightings_species ON sightings(species, observed_at);
CREATE INDEX IF NOT EXISTS ix_sightings_month ON sightings(strftime('%Y-%m', observed_at));

CREATE TABLE IF NOT EXISTS quarantine (
  content_hash TEXT PRIMARY KEY,
  payload TEXT NOT NULL,
  reason TEXT NOT NULL,
  first_seen TEXT NOT NULL);

CREATE TABLE IF NOT EXISTS sync_runs (
  run_id INTEGER PRIMARY KEY AUTOINCREMENT,
  started_at TEXT NOT NULL,
  finished_at TEXT,
  status TEXT NOT NULL,               -- running | ok | failed
  stats TEXT,
  error TEXT);
"""

class Sighting(BaseModel):
    id: str  # required
    date: datetime  # required
    location: Optional[str] = None
    species: Optional[str] = None
    latinName: Optional[str] = None

    @field_validator("id", "location", "species", "latinName")
    @classmethod
    def strip_blank(cls, v):
        if v is None:
            return None
        v = v.strip()
        return v or None  # plain whitespace becomes None

def completeness(row: dict) -> int:
    return sum(row[f] != PLACEHOLDER for f in MERGE_FIELDS)


def clean(raw: dict) -> dict:
    """Validate one API record and return a row for the sightings table.

    Raises ValidationError/ValueError for records that cannot be used
    (missing id, missing/unparseable date, date in the future).
    """
    s = Sighting(**raw)
    if not s.id:
        raise ValueError("missing id")

    date = s.date
    if date.tzinfo is not None:  # store everything as naive UTC
        date = date.astimezone(timezone.utc).replace(tzinfo=None)
    if date > datetime.now(timezone.utc).replace(tzinfo=None):
        raise ValueError("date is in the future")
    row = {
        "id": s.id,
        "observed_at": date.isoformat(),
        "city": s.location.title() if s.location else PLACEHOLDER,
        "species": s.species or PLACEHOLDER,
        "latin_name": s.latinName or PLACEHOLDER,
    }
    row["completeness"] = completeness(row)
    return row


def merge(existing: dict, new: dict):
    """Combine two copies of the same sighting.

    Blank fields are filled from the copy that is not empty. If both copies have
    different real values, the existing value is kept and the disagreement is
    returned as a conflict.
    """
    merged = dict(existing)
    conflicts = []
    for f in MERGE_FIELDS:
        if existing[f] == PLACEHOLDER and new[f] != PLACEHOLDER:
            merged[f] = new[f]
        elif new[f] != PLACEHOLDER and new[f] != existing[f]:
            conflicts.append(f"{f}: kept {existing[f]!r}, ignored {new[f]!r}")
    if new["observed_at"] != existing["observed_at"]:
        conflicts.append(f"observed_at: kept {existing['observed_at']}, ignored {new['observed_at']}")
    merged["completeness"] = completeness(merged)
    return merged, conflicts


def connect(path) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=NORMAL")
    db.executescript(SCHEMA)
    return db


def content_hash(raw) -> str:
    return hashlib.sha256(json.dumps(raw, sort_keys=True, default=str).encode()).hexdigest()


def placeholders(n: int) -> str:
    return ",".join("?" * n)

def process_chunk(db, chunk, now, stats):
    hashed = {}
    for raw in chunk:
        h = content_hash(raw)
        if h in hashed:
            stats["unchanged"] += 1
        else:
            hashed[h] = raw

    seen = {r[0] for r in db.execute(
        f"SELECT content_hash FROM raw_sightings WHERE content_hash IN ({placeholders(len(hashed))})",
        list(hashed))}
    new = {h: raw for h, raw in hashed.items() if h not in seen}
    stats["unchanged"] += len(hashed) - len(new)
    if not new:
        return
    db.executemany(
        "INSERT OR IGNORE INTO raw_sightings (id, content_hash, payload, fetched_at) VALUES (?,?,?,?)",
        [(str(raw.get("id")) if isinstance(raw, dict) and raw.get("id") is not None else None,
          h, json.dumps(raw, default=str), now) for h, raw in new.items()])
    valid, quarantined = [], []
    for h, raw in new.items():
        try:
            valid.append((h, raw, clean(raw)))
        except (ValidationError, ValueError, TypeError) as e:
            stats["invalid"] += 1
            reason = "; ".join(f"{'.'.join(map(str, err['loc']))}: {err['msg']}" for err in e.errors()) \
                if isinstance(e, ValidationError) else str(e)
            quarantined.append((h, json.dumps(raw, default=str), f"invalid: {reason}", now))
    ids = list({row["id"] for _, _, row in valid})
    existing = {}
    if ids:
        existing = {r["id"]: dict(r) for r in db.execute(
            f"SELECT * FROM sightings WHERE id IN ({placeholders(len(ids))})", ids)}
    writes = []
    for h, raw, row in valid:
        found = existing.get(row["id"])
        if found is None:
            final, kind = row, "inserted"
        else:
            final, conflicts = merge(found, row)
            if conflicts:
                stats["conflicts"] += 1
                quarantined.append((h, json.dumps(raw, default=str),
                                    "conflict: " + "; ".join(conflicts), now))
            kind = "merged" if final != found else "duplicate_no_change"
        stats[kind] += 1
        if kind != "duplicate_no_change":
            existing[row["id"]] = final
            writes.append(final)
    db.executemany(
        """INSERT OR REPLACE INTO sightings
           (id, observed_at, city, species, latin_name, completeness)
           VALUES (:id,:observed_at,:city,:species,:latin_name,:completeness)""", writes)
    db.executemany(
        "INSERT OR IGNORE INTO quarantine (content_hash, payload, reason, first_seen) VALUES (?,?,?,?)",
        quarantined)


def load(db, records, chunk_size=CHUNK_SIZE) -> dict:
    stats = {"fetched": len(records), "inserted": 0, "merged": 0, "duplicate_no_change": 0,
             "conflicts": 0, "invalid": 0, "unchanged": 0}
    now = datetime.now(timezone.utc).isoformat()
    for i in range(0, len(records), chunk_size):
        with db:  # one transaction per chunk
            process_chunk(db, records[i:i + chunk_size], now, stats)
    return stats


def fetch_records() -> list:
    """GET all sightings, retrying transient failures with exponential backoff.
    Backoff waits 0s, 2s, 4s... so a struggling server isn't hammered, and Retry-After
    is respected when the server sends one. The (connect, read) timeout means a hung
    server can never freeze the sync."""
    session = requests.Session()
    retry = Retry(total=5, backoff_factor=1, status_forcelist=(429, 500, 502, 503, 504),
                  respect_retry_after_header=True)
    session.mount("https://", HTTPAdapter(max_retries=retry))
    resp = session.get(f"{BASE_URL}/tick-sightings", timeout=(5, 30))
    resp.raise_for_status()
    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"expected a list from the API, got {type(data).__name__}")
    return data


def sync(db, fetch=fetch_records) -> dict:
    """Run one sync and record it in sync_runs. If the fetch fails, the data
    already stored is left untouched and the failure is recorded."""
    started = datetime.now(timezone.utc).isoformat()
    run_id = db.execute("INSERT INTO sync_runs (started_at, status) VALUES (?, 'running')",
                        (started,)).lastrowid
    db.commit()
    try:
        stats = load(db, fetch())
    except Exception as e:
        db.execute("UPDATE sync_runs SET finished_at=?, status='failed', error=? WHERE run_id=?",
                   (datetime.now(timezone.utc).isoformat(), f"{type(e).__name__}: {e}", run_id))
        db.commit()
        raise
    db.execute("UPDATE sync_runs SET finished_at=?, status='ok', stats=? WHERE run_id=?",
               (datetime.now(timezone.utc).isoformat(), json.dumps(stats), run_id))
    db.commit()
    return stats




# --------------------------------------------------------------------------- #
# Self-test and benchmark (written by claude)
# --------------------------------------------------------------------------- #
def self_test():
    db = connect(":memory:")
    a = {"id": "X1", "date": "2024-01-01T10:00:00", "location": "London", "species": "Marsh tick", "latinName": ""}
    b = {"id": "X1", "date": "2024-01-01T10:00:00", "location": "", "species": "", "latinName": "Ixodes apronophorus"}
    c = {"id": "X1", "date": "2024-01-01T10:00:00", "location": "Leeds", "species": "Marsh tick",
         "latinName": "Ixodes apronophorus"}
    bad_date = {"id": "X2", "date": "not a date", "location": "Leeds", "species": "x", "latinName": "y"}
    future = {"id": "X3", "date": "2999-01-01T00:00:00", "location": "Leeds", "species": "x", "latinName": "y"}
    no_id = {"date": "2024-01-01T10:00:00", "location": "Leeds", "species": "x", "latinName": "y"}
    records = [a, b, b, c, bad_date, future, no_id]

    first = sync(db, lambda: records)
    print("first sync :", first)
    row = db.execute("SELECT city, species, latin_name, completeness FROM sightings").fetchone()
    assert tuple(row) == ("London", "Marsh tick", "Ixodes apronophorus", 3), tuple(row)
    assert first == {"fetched": 7, "inserted": 1, "merged": 1, "duplicate_no_change": 1,
                     "conflicts": 1, "invalid": 3, "unchanged": 1}, first

    second = sync(db, lambda: records)
    print("second sync:", second)
    assert second["inserted"] == second["merged"] == second["invalid"] == 0, second
    assert db.execute("SELECT COUNT(*) FROM raw_sightings").fetchone()[0] == 6  # nothing re-added

    def broken():
        raise requests.ConnectionError("simulated outage")
    try:
        sync(db, broken)
    except requests.ConnectionError:
        pass
    last = db.execute("SELECT status, error FROM sync_runs ORDER BY run_id DESC LIMIT 1").fetchone()
    assert last["status"] == "failed", tuple(last)
    assert db.execute("SELECT COUNT(*) FROM sightings").fetchone()[0] == 1  # data still there
    print("self-test passed")


CITIES = ["Birmingham", "Bristol", "Cardiff", "Edinburgh", "Glasgow", "Leeds", "Leicester",
          "Liverpool", "London", "Manchester", "Newcastle", "Nottingham", "Sheffield", "Southampton"]
SPECIES = [("Marsh tick", "Ixodes apronophorus"), ("Passerine tick", "Dermacentor frontalis"),
           ("Southern rodent tick", "Ixodes acuminatus"), ("Sheep tick", "Ixodes ricinus")]


def fake_records(n, seed=1):
    rnd = random.Random(seed)
    start = datetime(2010, 1, 1)
    span = int((datetime(2025, 12, 31) - start).total_seconds())
    out = []
    for i in range(n):
        species, latin = rnd.choice(SPECIES)
        out.append({"id": f"{i:020d}",
                    "date": (start + timedelta(seconds=rnd.randrange(span))).isoformat(),
                    "location": rnd.choice(CITIES),
                    "species": "" if rnd.random() < 0.05 else species,  # 5% incomplete
                    "latinName": latin})
        if rnd.random() < 0.02:
            dup = dict(out[-1])
            dup["latinName"] = ""
            out.append(dup)
    return out


def timed(db, label, sql, params=()):
    t = time.perf_counter()
    rows = db.execute(sql, params).fetchall()
    print(f"  {label}: {len(rows)} rows in {(time.perf_counter() - t) * 1000:.1f} ms")


def benchmark(n):
    path = Path(__file__).parent / "ticks_bench.db"
    for suffix in ("", "-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)
    db = connect(path)
    records = fake_records(n)
    print(f"Generated {len(records):,} records")

    t = time.perf_counter()
    stats = load(db, records)
    dt = time.perf_counter() - t
    print(f"Initial load: {dt:.1f}s ({len(records) / dt:,.0f} records/s) {stats}")

    t = time.perf_counter()
    stats = load(db, records)
    print(f"Repeat sync : {time.perf_counter() - t:.1f}s, nothing changed {stats}")

    print("Queries:")
    timed(db, "date range    ", "SELECT * FROM sightings WHERE observed_at BETWEEN ? AND ?",
          ("2020-01-01", "2020-12-31T23:59:59"))
    timed(db, "city + range  ", "SELECT * FROM sightings WHERE city=? AND observed_at >= ?", ("London", "2024-01-01"))
    timed(db, "by city       ", "SELECT city, COUNT(*) FROM sightings GROUP BY city")
    timed(db, "monthly trend ", "SELECT strftime('%Y-%m', observed_at) m, COUNT(*) FROM sightings GROUP BY m")
    plan = db.execute("EXPLAIN QUERY PLAN SELECT * FROM sightings WHERE observed_at BETWEEN ? AND ?",
                      ("2020-01-01", "2020-12-31")).fetchall()
    print("  date-range plan:", [r["detail"] for r in plan])
    db.close()
    for suffix in ("", "-wal", "-shm"):
        Path(str(path) + suffix).unlink(missing_ok=True)


# --------------------------------------------------------------------------- #




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--self-test", action="store_true", help="run the built-in tests and exit")
    parser.add_argument("--benchmark", type=int, metavar="N", help="time a load of N fake records and exit")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        sys.exit(0)
    if args.benchmark:
        benchmark(args.benchmark)
        sys.exit(0)

    db = connect(DB_PATH)
    try:
        stats = sync(db)
    except Exception as e:
        print(f"Sync failed: {type(e).__name__}: {e}")
        print("Existing data in the database is unchanged.")
        sys.exit(1)

    print("Sync stats:", stats)
    for table in ("raw_sightings", "sightings", "quarantine", "sync_runs"):
        print(f"{table}: {db.execute(f'SELECT COUNT(*) FROM {table}').fetchone()[0]} rows")
    print(f"Database file: {DB_PATH}")
