"""Tick sightings endpoint: search and filter data fetched by Ingest.py.

    currently only works on a localhost machine
Endpoints
    GET /sightings        search with filters (time range, city, species), sort, pagination
    GET /sightings/{id}   one sighting
    GET /locations        cities in the data, with sighting counts
    GET /species          species in the data, with sighting counts
    GET /sync/status      when the data was last refreshed, and whether that worked
    GET /health           is the database reachable?
    GET /reports/by-region    sightings per city, with percentages
    GET /reports/by-species   sightings per species, with percentages
    GET /reports/trends       sightings per week or month (?interval=weekly|monthly)
"""
import json
import os
import sqlite3
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Literal, Optional

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

from Ingest import MERGE_FIELDS

DB_PATH = Path(os.environ.get("TICKS_DB", Path(__file__).parent / "ticks.db"))
MAX_LIMIT = 500
FULLY_COMPLETE = len(MERGE_FIELDS)  # sightings with all fields filled

app = FastAPI(
    title="Tick Sightings API",
    description="Search and filter tick sightings by a given time range and location."
                "Also reports on sightings per region and trends over time",
)


def get_db():
    """One connection per request. (all read only)"""
    if not DB_PATH.exists():
        raise HTTPException(503, "Database not found. Run `python Ingest.py` first.")
    db = sqlite3.connect(f"{DB_PATH.resolve().as_uri()}?mode=ro", uri=True, check_same_thread=False)
    db.row_factory = sqlite3.Row
    try:
        yield db
    finally:
        db.close()

@app.exception_handler(sqlite3.Error)
def database_error(request: Request, exc: sqlite3.Error):
    return JSONResponse(status_code=503, content={"detail": f"Database error: {exc}"})

class SightingOut(BaseModel):
    id: str
    observed_at: datetime
    city: str
    species: str
    latin_name: str
    completeness: int  #how many of city / species / latin_name are filled in (number between 0-3)


class SightingPage(BaseModel):
    total: int
    limit: int
    offset: int
    results: list[SightingOut]


class RegionRow(BaseModel):
    region: str  # each city is treated as a region for this (unsure if we had to find regions cities were in)
    sightings: int
    percent: float


class RegionReport(BaseModel):
    total: int
    regions: list[RegionRow]


class SpeciesRow(BaseModel):
    species: str
    sightings: int
    percent: float


class SpeciesReport(BaseModel):
    total: int
    species: list[SpeciesRow]


class TrendPoint(BaseModel):
    period: str
    sightings: int


class TrendReport(BaseModel):
    interval: str
    total: int
    points: list[TrendPoint]


def parse_when(value: str, field: str):
    """Convert a user-supplied ISO date or datetime string into a datetime.

       Accepts "2022-12-31" or "2022-12-31T14:30:00" (time zone offsets are allowed).
       Offsets are converted to UTC and then dropped after, because the database stores
       UTC times without timezones attached.

       Returns (datetime, date_only), where date_only is True when no time was given.

       Raises HTTP 422 naming `field` if the text is not in a valid format (ISO-date).
       """
    value = value.strip()
    try:
        dt = datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(
            422, f"'{field}' must be an ISO date or datetime, e.g. 2022-01-31 or 2022-01-31T14:30:00")
    if dt.tzinfo is not None:  # the database stores naive UTC
        dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
    return dt, ("T" not in value and " " not in value)

class Filters:
    """The WHERE clause and parameters built from the query string."""

    def __init__(
        self,
        start: Optional[str] = Query(None, description="From this date/time (inclusive). e.g. 2022-01-01"),
        end: Optional[str] = Query(None, description="Up to this date/time. A plain date includes that whole day."),
        city: list[str] = Query(default=[], description="One or more cities, case-insensitive. Repeat to add more."),
        species: list[str] = Query(default=[], description="One or more species, case-insensitive."),
        include_incomplete: bool = Query(True, description="False hides sightings with any 'Incomplete' field."),
    ):
        clauses, params = [], []

        start_dt = end_dt = None
        end_date_only = False
        if start:
            start_dt, _ = parse_when(start, "start")
        if end:
            end_dt, end_date_only = parse_when(end, "end")
        if start_dt and end_dt and start_dt > end_dt:
            raise HTTPException(422, "'start' must not be after 'end'")
        self.start_dt, self.end_dt = start_dt, end_dt

        if start_dt:
            clauses.append("observed_at >= ?")
            params.append(start_dt.isoformat())
        if end_dt:
            if end_date_only:  # dates specified here include the whole day
                clauses.append("observed_at < ?")
                params.append((end_dt + timedelta(days=1)).isoformat())
            else:
                clauses.append("observed_at <= ?")
                params.append(end_dt.isoformat())

        cities = [c.strip().title() for c in city if c.strip()]  # stored title-cased by loader
        if cities:
            clauses.append(f"city IN ({','.join('?' * len(cities))})")
            params += cities

        species_list = [s.strip() for s in species if s.strip()]
        if species_list:  # NOCASE matches case-insensitive
            clauses.append(f"species COLLATE NOCASE IN ({','.join('?' * len(species_list))})")
            params += species_list

        if not include_incomplete:
            clauses.append("completeness = ?")
            params.append(FULLY_COMPLETE)

        self.where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
        self.params = params








#Endpoints

@app.get("/", include_in_schema=False)
def home():
    """The search page. The JSON API lives under /sightings; /docs has the API reference."""
    return FileResponse(Path(__file__).parent / "index.html")


@app.get("/sightings", response_model=SightingPage, summary="Search sightings")
def search_sightings(
    filters: Filters = Depends(),
    sort: Literal["date_desc", "date_asc"] = Query("date_desc"),
    limit: int = Query(50, ge=1, le=MAX_LIMIT, description=f"Page size (max {MAX_LIMIT})"),
    offset: int = Query(0, ge=0),
    db: sqlite3.Connection = Depends(get_db),
):

    total = db.execute(f"SELECT COUNT(*) FROM sightings {filters.where}", filters.params).fetchone()[0]
    direction = "DESC" if sort == "date_desc" else "ASC"
    rows = db.execute(
        f"""SELECT id, observed_at, city, species, latin_name, completeness
            FROM sightings {filters.where}
            ORDER BY observed_at {direction}, id
            LIMIT ? OFFSET ?""",
        [*filters.params, limit, offset],
    ).fetchall()
    return {"total": total, "limit": limit, "offset": offset, "results": [dict(r) for r in rows]}


@app.get("/sightings/{sighting_id}", response_model=SightingOut, summary="Get one sighting")
def get_sighting(sighting_id: str, db: sqlite3.Connection = Depends(get_db)):
    row = db.execute(
        "SELECT id, observed_at, city, species, latin_name, completeness FROM sightings WHERE id = ?",
        (sighting_id,)).fetchone()
    if row is None:
        raise HTTPException(404, f"No sighting with id '{sighting_id}'")
    return dict(row)

@app.get("/locations", summary="Cities in the data")
def list_locations(db: sqlite3.Connection = Depends(get_db)):
    rows = db.execute("SELECT city, COUNT(*) AS sightings FROM sightings GROUP BY city ORDER BY city").fetchall()
    return [dict(r) for r in rows]


@app.get("/species", summary="Species in the data")
def list_species(db: sqlite3.Connection = Depends(get_db)):
    rows = db.execute(
        "SELECT species, latin_name, COUNT(*) AS sightings FROM sightings "
        "GROUP BY species, latin_name ORDER BY species").fetchall()
    return [dict(r) for r in rows]

MAX_FILLED_PERIODS = 5000  # limit on how many empty periods the report can generate

MONTH_EXPR = "strftime('%Y-%m', observed_at)" # expression used for months
# Monday of the week each sighting falls in.
# Did not use strftime('%W') given it's year-boundary quirks (sometimes can weirdly assign week numbers towards end of the year,
# and the labels sort in date order.
WEEK_EXPR = ("date(observed_at, '-' || ((CAST(strftime('%w', observed_at) AS INTEGER) + 6) % 7) || ' days')") # expression used for weeks


def percent(part: int, whole: int) -> float:
    return round(100 * part / whole, 1) if whole else 0.0


def period_start(dt: datetime, interval: str) -> date:
    d = dt.date()
    return d.replace(day=1) if interval == "monthly" else d - timedelta(days=d.weekday())


def next_period(d: date, interval: str) -> date:
    if interval == "monthly":
        return date(d.year + d.month // 12, d.month % 12 + 1, 1)
    return d + timedelta(days=7)


def period_label(d: date, interval: str) -> str:
    return d.strftime("%Y-%m") if interval == "monthly" else d.isoformat()


def parse_period(label: str, interval: str) -> date:
    return date(int(label[:4]), int(label[5:7]), 1) if interval == "monthly" else date.fromisoformat(label)


def fill_gaps(points: list, interval: str, start_dt: Optional[datetime], end_dt: Optional[datetime]) -> list:
    """Add zero-count periods so a chart gets a continuous series, also taking into account
    the callers time period request (if there is one)"""
    keys = [parse_period(p["period"], interval) for p in points]
    if not keys and not (start_dt and end_dt):
        return points
    first = period_start(start_dt, interval) if start_dt else min(keys)
    last = period_start(end_dt, interval) if end_dt else max(keys)
    counts = {p["period"]: p["sightings"] for p in points}
    filled, current = [], first
    while current <= last:
        if len(filled) >= MAX_FILLED_PERIODS:
            raise HTTPException(422, "Date range is too large to fill with empty periods. "
                                     "Narrow the range")
        label = period_label(current, interval)
        filled.append({"period": label, "sightings": counts.get(label, 0)})
        current = next_period(current, interval)
    return filled





@app.get("/reports/by-region", response_model=RegionReport, summary="Sightings per region")
def report_by_region(filters: Filters = Depends(), db: sqlite3.Connection = Depends(get_db)):
    rows = db.execute(
        f"""SELECT city AS region, COUNT(*) AS sightings FROM sightings {filters.where}
            GROUP BY city ORDER BY sightings DESC, city""", filters.params).fetchall()
    total = sum(r["sightings"] for r in rows)
    return {"total": total,
            "regions": [{"region": r["region"], "sightings": r["sightings"],
                         "percent": percent(r["sightings"], total)} for r in rows]}


@app.get("/reports/by-species", response_model=SpeciesReport, summary="Sightings per species")
def report_by_species(filters: Filters = Depends(), db: sqlite3.Connection = Depends(get_db)):
    rows = db.execute(
        f"""SELECT species, COUNT(*) AS sightings FROM sightings {filters.where}
            GROUP BY species ORDER BY sightings DESC, species""", filters.params).fetchall()
    total = sum(r["sightings"] for r in rows)
    return {"total": total,
            "species": [{"species": r["species"], "sightings": r["sightings"],
                         "percent": percent(r["sightings"], total)} for r in rows]}


@app.get("/reports/trends", response_model=TrendReport, summary="Sightings over time")
def report_trends(
    filters: Filters = Depends(),
    interval: Literal["weekly", "monthly"] = Query("monthly"),
    fill_gaps_: bool = Query(True, alias="fill_gaps", description="Include periods with zero sightings"),
    db: sqlite3.Connection = Depends(get_db),
):
    expr = MONTH_EXPR if interval == "monthly" else WEEK_EXPR
    rows = db.execute(
        f"SELECT {expr} AS period, COUNT(*) AS sightings FROM sightings {filters.where} "
        "GROUP BY period ORDER BY period", filters.params).fetchall()
    points = [{"period": r["period"], "sightings": r["sightings"]} for r in rows]
    if fill_gaps_:
        points = fill_gaps(points, interval, filters.start_dt, filters.end_dt)
    return {"interval": interval, "total": sum(p["sightings"] for p in points), "points": points}


@app.get("/sync/status", summary="When the data was last refreshed")
def sync_status(db: sqlite3.Connection = Depends(get_db)):
    def latest(where: str = ""):
        r = db.execute(
            f"SELECT run_id, started_at, finished_at, status, stats, error FROM sync_runs {where} "
            "ORDER BY run_id DESC LIMIT 1").fetchone()
        if r is None:
            return None
        out = dict(r)
        out["stats"] = json.loads(out["stats"]) if out["stats"] else None
        return out

    return {
        "total_sightings": db.execute("SELECT COUNT(*) FROM sightings").fetchone()[0],
        "last_run": latest(),
        "last_successful_run": latest("WHERE status = 'ok'"),
    }

@app.get("/health", summary="Is the database reachable?")
def health(db: sqlite3.Connection = Depends(get_db)):
    db.execute("SELECT 1").fetchone()
    return {"status": "healthy"}
