"""
Employee Attendance & Analytics API

Run:  uvicorn app.main:app --port 8000
Env:  MONGO_URI, MONGO_DB (a local .env is loaded for convenience; real environment variables win)

Layout of this file
   1. Configuration and the MongoDB client
   2. Errors
   3. Time helpers: epoch ms <-> datetime, IST, the attendance date (R1)
   4. Business rules R2-R5
   5. Request models and validation
   6. Serialisation: MongoDB document -> API JSON
   7. Indexes and startup
   8. System and employee endpoints
   9. Attendance endpoints: punch-in, punch-out, list, regularize
  10. Aggregation pipeline builders (shared by the analytics and explain endpoints)
  11. Analytics endpoints
  12. Explain endpoint
"""
import calendar
import json
import logging
import os
import re
from contextlib import asynccontextmanager
from datetime import date, datetime, time, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from typing import Annotated, Any, Literal, Optional

from bson import Decimal128, json_util
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Path, Query
from pydantic import AfterValidator, BaseModel, Field, StrictStr, model_validator
from pymongo import ASCENDING, DESCENDING, MongoClient, ReturnDocument
from pymongo.errors import DuplicateKeyError, PyMongoError

log = logging.getLogger("attendance")

# --------------------------------------------------------------------------- #
# 1. Configuration and the MongoDB client
# --------------------------------------------------------------------------- #
load_dotenv(override=False)  # .env only fills gaps; a real environment variable always wins

UTC = timezone.utc
IST = timezone(timedelta(hours=5, minutes=30))
EPOCH = datetime(1970, 1, 1, tzinfo=UTC)

# tz_aware=True: PyMongo returns aware UTC datetimes instead of naive ones, so every
# datetime in this file is unambiguous and can be converted to IST safely.
client = MongoClient(
    os.getenv("MONGO_URI", "mongodb://localhost:27017"),
    tz_aware=True,
    tzinfo=UTC,
    serverSelectionTimeoutMS=3000,
)
db = client[os.getenv("MONGO_DB", "attendance_db")]
employees = db["employees"]
logs = db["attendance_logs"]

PRESENCE_STATUSES = ("PRESENT", "WFH", "ON_DUTY")  # R6: these count as present
ABSENCE_STATUSES = ("ABSENT", "LEAVE")
Status = Literal["PRESENT", "ABSENT", "LEAVE", "WFH", "ON_DUTY"]
PresenceStatus = Literal["PRESENT", "WFH", "ON_DUTY"]

GRACE_SECONDS = 10 * 60          # R2
MIN_OVERTIME_MINUTES = 30        # R3
HALF_DAY_BELOW_HOURS = 4.5       # R5
MAX_SHIFT = timedelta(hours=24)  # punch-out must be within 24 h of punch-in
MIN_EPOCH_MS = 100_000_000_000   # anything smaller looks like seconds -> 422
MAX_EPOCH_MS = 4_102_444_800_000
TREND_MAX_DAYS = 92

DATE_RE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")
MONTH_PATTERN = r"^[0-9]{4}-(0[1-9]|1[0-2])$"
HHMM_PATTERN = r"^([01][0-9]|2[0-3]):[0-5][0-9]$"


# --------------------------------------------------------------------------- #
# 2. Errors
# --------------------------------------------------------------------------- #
def unprocessable(msg: str, loc: tuple = ("body",)) -> HTTPException:
    """422 shaped like FastAPI's own validation errors: {"detail": [{loc, msg, type}]}."""
    return HTTPException(422, detail=[{"loc": list(loc), "msg": msg, "type": "value_error"}])


def not_found(msg: str) -> HTTPException:
    return HTTPException(404, detail=msg)


def conflict(msg: str) -> HTTPException:
    return HTTPException(409, detail=msg)


# --------------------------------------------------------------------------- #
# 3. Time helpers
# --------------------------------------------------------------------------- #
def ms_to_dt(ms: int) -> datetime:
    """Epoch ms -> aware UTC datetime, truncated to whole seconds (R1)."""
    return datetime.fromtimestamp(ms // 1000, tz=UTC)


def now_utc() -> datetime:
    """The current instant, truncated to whole seconds (R1)."""
    return datetime.now(UTC).replace(microsecond=0)


def dt_to_ms(value: Optional[datetime]) -> Optional[int]:
    """Aware or naive (treated as UTC) datetime -> epoch ms, using integer arithmetic only."""
    if value is None:
        return None
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return (value - EPOCH) // timedelta(milliseconds=1)


def parse_date(value: str) -> date:
    """Strict YYYY-MM-DD. date.fromisoformat alone would also accept '20260105'."""
    if not DATE_RE.match(value):
        raise ValueError("must be a date in YYYY-MM-DD format")
    return date.fromisoformat(value)  # raises ValueError for 2026-02-30


def date_param(value: str, name: str, where: str = "query") -> date:
    try:
        return parse_date(value)
    except ValueError as exc:
        raise unprocessable(str(exc), (where, name))


def month_bounds(month: str) -> tuple[str, str]:
    """'2026-07' -> ('2026-07-01', '2026-07-31')."""
    year, mon = map(int, month.split("-"))
    return f"{month}-01", f"{month}-{calendar.monthrange(year, mon)[1]:02d}"


def hhmm(value: str) -> time:
    hours, minutes = map(int, value.split(":"))
    return time(hours, minutes)


def is_overnight(emp: dict) -> bool:
    # Zero-padded HH:MM strings compare correctly as text.
    return emp["shift_end"] <= emp["shift_start"]


def attendance_date(punch_in: datetime, emp: dict) -> date:
    """R1: the IST calendar date of the punch-in, except that on an overnight shift a
    punch-in earlier than shift_end belongs to the shift that started the day before."""
    local = punch_in.astimezone(IST)
    day = local.date()
    if is_overnight(emp) and local.time() < hhmm(emp["shift_end"]):
        day -= timedelta(days=1)
    return day


def shift_start_at(day: date, emp: dict) -> datetime:
    return datetime.combine(day, hhmm(emp["shift_start"]), tzinfo=IST)


def shift_end_at(day: date, emp: dict) -> datetime:
    end = datetime.combine(day, hhmm(emp["shift_end"]), tzinfo=IST)
    return end + timedelta(days=1) if is_overnight(emp) else end  # R1: overnight ends next day


# --------------------------------------------------------------------------- #
# 4. Business rules R2-R5 (all inputs are already truncated to whole seconds)
# --------------------------------------------------------------------------- #
def seconds_between(start: datetime, end: datetime) -> int:
    return (end - start) // timedelta(seconds=1)  # floor, exact integer


def round_half_up(value: Decimal, places: int) -> Decimal:
    return value.quantize(Decimal(1).scaleb(-places), rounding=ROUND_HALF_UP)


def compute_late_minutes(punch_in: datetime, day: date, emp: dict) -> int:
    """R2: late only if strictly more than 10:00 after shift start; then the whole
    minutes since shift start (not since the end of the grace period)."""
    elapsed = seconds_between(shift_start_at(day, emp), punch_in)
    return elapsed // 60 if elapsed > GRACE_SECONDS else 0


def compute_work_hours(punch_in: datetime, punch_out: datetime) -> float:
    """R4: seconds / 3600, 2 decimals half-up. Decimal avoids float and banker's rounding."""
    seconds = seconds_between(punch_in, punch_out)
    return float(round_half_up(Decimal(seconds) / Decimal(3600), 2))


def compute_overtime(punch_out: datetime, day: date, emp: dict) -> int:
    """R3: whole minutes after shift end, only when that is at least 30."""
    minutes = seconds_between(shift_end_at(day, emp), punch_out) // 60
    return minutes if minutes >= MIN_OVERTIME_MINUTES else 0


def derived_fields(status: str, punch_in, punch_out, day: date, emp: dict) -> dict:
    """The four API-computed fields, following the table in DATA_MODEL.md section 3."""
    if status in ABSENCE_STATUSES or punch_in is None:
        return {"work_hours": None, "late_minutes": 0, "overtime_minutes": 0, "half_day": False}
    fields = {
        "work_hours": None,
        "late_minutes": compute_late_minutes(punch_in, day, emp),
        "overtime_minutes": 0,
        "half_day": False,
    }
    if punch_out is not None:
        hours = compute_work_hours(punch_in, punch_out)
        fields["work_hours"] = hours
        fields["overtime_minutes"] = compute_overtime(punch_out, day, emp)
        fields["half_day"] = hours < HALF_DAY_BELOW_HOURS  # R5 uses the rounded value
    return fields


# --------------------------------------------------------------------------- #
# 5. Request models
#
# Fields typed `EpochMs = None` (rather than Optional[EpochMs]) may be omitted, but an
# explicit null is rejected with 422, because the contract does not make them nullable.
# Unknown fields, including derived ones such as work_hours, are ignored.
# --------------------------------------------------------------------------- #
def _check_date(value: str) -> str:
    parse_date(value)
    return value


EpochMs = Annotated[int, Field(strict=True, ge=MIN_EPOCH_MS, le=MAX_EPOCH_MS)]  # strict: no floats/strings/bools
DateStr = Annotated[StrictStr, AfterValidator(_check_date)]
HHMM = Annotated[StrictStr, Field(pattern=HHMM_PATTERN)]


class EmployeeCreate(BaseModel):
    emp_code: Annotated[StrictStr, Field(pattern=r"^EMP[0-9]{4,6}$")]
    name: Annotated[StrictStr, Field(min_length=1, max_length=100)]
    email: Annotated[StrictStr, Field(max_length=120, pattern=r"^[^@\s]+@[^@\s]+\.[^@\s]+$")]
    department: Annotated[StrictStr, Field(min_length=1, max_length=50)]
    shift_start: HHMM = "09:30"
    shift_end: HHMM = "18:30"
    joined_on: DateStr

    @model_validator(mode="after")
    def shift_must_have_length(self):
        if self.shift_start == self.shift_end:
            raise ValueError("shift_start must differ from shift_end")
        return self


class PunchInRequest(BaseModel):
    emp_code: StrictStr
    punched_at: EpochMs = None
    status: PresenceStatus = "PRESENT"


class PunchOutRequest(BaseModel):
    emp_code: StrictStr
    punched_at: EpochMs = None


class RegularizeRequest(BaseModel):
    status: Status = None
    punch_in: EpochMs = None
    punch_out: EpochMs = None
    reason: Annotated[StrictStr, Field(min_length=5, max_length=200)]
    regularized_by: Annotated[StrictStr, Field(min_length=1, max_length=50)]


# --------------------------------------------------------------------------- #
# 6. Serialisation: _id never leaves the service, instants leave as epoch ms
# --------------------------------------------------------------------------- #
def employee_out(doc: dict) -> dict:
    return {
        "emp_code": doc["emp_code"],
        "name": doc.get("name"),
        "email": doc.get("email"),
        "department": doc.get("department"),
        "shift_start": doc.get("shift_start"),
        "shift_end": doc.get("shift_end"),
        "joined_on": doc.get("joined_on"),
        "created_at": dt_to_ms(doc.get("created_at")),
    }


def _instant_or_value(value: Any) -> Any:
    return dt_to_ms(value) if isinstance(value, datetime) else value


def history_out(entry: dict) -> dict:
    changes = entry.get("changes") or {}
    return {
        "at": dt_to_ms(entry.get("at")),
        "by": entry.get("by"),
        "reason": entry.get("reason"),
        # punch_in / punch_out from/to are BSON dates inside a nested object; convert them too.
        "changes": {
            field: {"from": _instant_or_value(ch.get("from")), "to": _instant_or_value(ch.get("to"))}
            for field, ch in changes.items()
        },
    }


def record_out(doc: dict) -> dict:
    """Legacy records may omit history / half_day / late / overtime: apply the documented defaults."""
    return {
        "emp_code": doc["emp_code"],
        "date": doc["date"],
        "status": doc["status"],
        "punch_in": dt_to_ms(doc.get("punch_in")),
        "punch_out": dt_to_ms(doc.get("punch_out")),
        "work_hours": doc.get("work_hours"),
        "late_minutes": doc.get("late_minutes") or 0,
        "overtime_minutes": doc.get("overtime_minutes") or 0,
        "half_day": bool(doc.get("half_day", False)),
        "history": [history_out(h) for h in (doc.get("history") or [])],
    }


# --------------------------------------------------------------------------- #
# 7. Indexes and startup
# --------------------------------------------------------------------------- #
INDEXES = {
    "employees": [
        # Identity, and the race-safe 409 for duplicate creates. Also every $lookup by emp_code.
        ([("emp_code", ASCENDING)], {"name": "uniq_emp_code", "unique": True}),
        # GET /employees?department= sorted by emp_code; department summary and trend.
        ([("department", ASCENDING), ("emp_code", ASCENDING)], {"name": "department_emp_code"}),
    ],
    "attendance_logs": [
        # One record per employee per day: the race-safe 409 for punch-in. Also serves the
        # emp_code filter on GET /attendance, punch-out, regularize, and every per-employee $lookup.
        ([("emp_code", ASCENDING), ("date", ASCENDING)], {"name": "uniq_emp_code_date", "unique": True}),
        # GET /attendance sort (date desc, emp_code asc) and month/date ranges across all employees.
        ([("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "date_desc_emp_code"}),
        # GET /attendance?status= with the same sort, and its total count.
        ([("status", ASCENDING), ("date", DESCENDING), ("emp_code", ASCENDING)], {"name": "status_date_emp_code"}),
    ],
}


def ensure_indexes() -> None:
    """create_index is idempotent: an existing index with the same spec is left alone."""
    for collection, specs in INDEXES.items():
        for keys, options in specs:
            try:
                db[collection].create_index(keys, **options)
            except PyMongoError as exc:  # keep starting: /health and the rest still work
                log.error("could not create index %s on %s: %s", options["name"], collection, exc)


@asynccontextmanager
async def lifespan(_app: FastAPI):
    ensure_indexes()
    yield


app = FastAPI(title="Employee Attendance & Analytics API", version="2.0.0", lifespan=lifespan)


# --------------------------------------------------------------------------- #
# 8. System and employee endpoints
# --------------------------------------------------------------------------- #
@app.get("/health")
def health():
    try:
        client.admin.command("ping")
    except PyMongoError:
        raise HTTPException(503, detail="MongoDB is not reachable")
    return {"status": "ok"}


@app.post("/employees", status_code=201)
def create_employee(body: EmployeeCreate):
    doc = body.model_dump()
    doc["created_at"] = datetime.now(UTC)
    try:
        employees.insert_one(doc)  # the unique index decides; no check-then-insert race
    except DuplicateKeyError:
        raise conflict(f"emp_code {body.emp_code} already exists")
    return employee_out(doc)


@app.get("/employees")
def list_employees(
    department: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    query = {} if department is None else {"department": department}
    cursor = (
        employees.find(query)
        .sort("emp_code", ASCENDING)
        .skip((page - 1) * page_size)
        .limit(page_size)
    )
    return {
        "items": [employee_out(doc) for doc in cursor],
        "total": employees.count_documents(query),  # R10: the filtered total
        "page": page,
        "page_size": page_size,
    }


# --------------------------------------------------------------------------- #
# 9. Attendance endpoints
# --------------------------------------------------------------------------- #
def get_employee(emp_code: str) -> dict:
    emp = employees.find_one({"emp_code": emp_code})
    if emp is None:
        raise not_found(f"employee {emp_code} not found")
    return emp


@app.post("/attendance/punch-in", status_code=201)
def punch_in(body: PunchInRequest):
    emp = get_employee(body.emp_code)
    punched = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()
    day = attendance_date(punched, emp)
    doc = {
        "emp_code": emp["emp_code"],
        "date": day.isoformat(),
        "status": body.status,
        "punch_in": punched,
        "punch_out": None,
        **derived_fields(body.status, punched, None, day, emp),
        "history": [],
    }
    try:
        # Concurrency: no read first. The unique (emp_code, date) index lets exactly one
        # insert through; every other identical request gets DuplicateKeyError -> 409.
        logs.insert_one(doc)
    except DuplicateKeyError:
        raise conflict(f"{body.emp_code} already punched in for {doc['date']}")
    return record_out(doc)


@app.post("/attendance/punch-out")
def punch_out(body: PunchOutRequest):
    emp = get_employee(body.emp_code)
    punched = ms_to_dt(body.punched_at) if body.punched_at is not None else now_utc()

    # The most recent record whose punch_in is at or before punched_at. A record's date
    # follows its punch_in (R1), so walking (emp_code, date) newest-first finds it in a
    # key or two, including an overnight record whose date is the previous day.
    record = logs.find_one(
        {"emp_code": emp["emp_code"], "punch_in": {"$lte": punched}},
        sort=[("date", DESCENDING)],
    )
    if record is None or record.get("punch_out") is not None:
        # If the employee has an open record that started after punched_at, the client's
        # time is simply before its punch-in: that is the contract's 422, not a 404/409.
        if logs.find_one({"emp_code": emp["emp_code"], "punch_in": {"$gt": punched}, "punch_out": None}):
            raise unprocessable("punched_at must be after punch_in", ("body", "punched_at"))
        if record is None:
            raise not_found(f"no punch-in found for {emp['emp_code']}")
        raise conflict(f"{emp['emp_code']} already punched out for {record['date']}")

    if punched <= record["punch_in"]:
        raise unprocessable("punched_at must be after punch_in", ("body", "punched_at"))
    if punched - record["punch_in"] > MAX_SHIFT:
        raise unprocessable("punched_at must be within 24 hours of punch_in", ("body", "punched_at"))

    day = parse_date(record["date"])
    fields = {"punch_out": punched, **derived_fields(record["status"], record["punch_in"], punched, day, emp)}
    # Concurrency: the filter only matches while punch_out is still null, so of two
    # simultaneous punch-outs exactly one modifies the record; the other gets None -> 409.
    updated = logs.find_one_and_update(
        {"_id": record["_id"], "punch_out": None},
        {"$set": fields},
        return_document=ReturnDocument.AFTER,
    )
    if updated is None:
        raise conflict(f"{emp['emp_code']} already punched out for {record['date']}")
    return record_out(updated)


def attendance_list_query(emp_code, date_from, date_to, status) -> dict:
    """Shared by GET /attendance and its explain."""
    query: dict = {}
    if emp_code is not None:
        query["emp_code"] = emp_code
    start = date_param(date_from, "date_from") if date_from is not None else None
    end = date_param(date_to, "date_to") if date_to is not None else None
    if start and end and start > end:
        raise unprocessable("date_from must not be after date_to", ("query", "date_from"))
    if start or end:
        query["date"] = {}
        if start:
            query["date"]["$gte"] = date_from
        if end:
            query["date"]["$lte"] = date_to
    if status is not None:
        query["status"] = status
    return query


ATTENDANCE_SORT = [("date", DESCENDING), ("emp_code", ASCENDING)]


@app.get("/attendance")
def list_attendance(
    emp_code: Optional[str] = None,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    query = attendance_list_query(emp_code, date_from, date_to, status)
    # The database sorts (using date_desc_emp_code) and pages; only one page is ever loaded.
    cursor = logs.find(query).sort(ATTENDANCE_SORT).skip((page - 1) * page_size).limit(page_size)
    # Unfiltered: read the count from collection metadata instead of counting 100k docs.
    total = logs.count_documents(query) if query else logs.estimated_document_count()
    return {"items": [record_out(d) for d in cursor], "total": total, "page": page, "page_size": page_size}


TRACKED_FIELDS = ("status", "punch_in", "punch_out", "work_hours", "late_minutes", "overtime_minutes", "half_day")


@app.patch("/attendance/{emp_code}/{date}")
def regularize_attendance(body: RegularizeRequest, emp_code: str, date_str: str = Path(alias="date")):
    day = date_param(date_str, "date", where="path")
    emp = get_employee(emp_code)
    record = logs.find_one({"emp_code": emp_code, "date": date_str})
    if record is None:
        raise not_found(f"no attendance record for {emp_code} on {date_str}")

    old = {
        "status": record["status"],
        "punch_in": record.get("punch_in"),
        "punch_out": record.get("punch_out"),
        "work_hours": record.get("work_hours"),
        "late_minutes": record.get("late_minutes") or 0,
        "overtime_minutes": record.get("overtime_minutes") or 0,
        "half_day": bool(record.get("half_day", False)),
    }

    # Work out the final status and punch times. Omitted fields keep their stored value.
    status = body.status if body.status is not None else old["status"]
    new_in = ms_to_dt(body.punch_in) if body.punch_in is not None else None
    new_out = ms_to_dt(body.punch_out) if body.punch_out is not None else None
    if status in ABSENCE_STATUSES:
        if new_in is not None or new_out is not None:
            raise unprocessable(f"{status} has no punch times; do not send punch_in or punch_out")
        punch_in_at = punch_out_at = None
    else:
        punch_in_at = new_in or old["punch_in"]
        punch_out_at = new_out or old["punch_out"]
        if punch_in_at is None:
            raise unprocessable(f"status {status} requires a punch_in", ("body", "punch_in"))
        if attendance_date(punch_in_at, emp) != day:
            raise unprocessable(f"punch_in must stay on the attendance date {date_str}", ("body", "punch_in"))
        if punch_out_at is not None:
            if punch_out_at <= punch_in_at:
                raise unprocessable("punch_out must be after punch_in", ("body", "punch_out"))
            if punch_out_at - punch_in_at > MAX_SHIFT:
                raise unprocessable("punch_out must be within 24 hours of punch_in", ("body", "punch_out"))

    if (status, punch_in_at, punch_out_at) == (old["status"], old["punch_in"], old["punch_out"]):
        raise unprocessable("the request changes nothing")

    new = {
        "status": status,
        "punch_in": punch_in_at,
        "punch_out": punch_out_at,
        **derived_fields(status, punch_in_at, punch_out_at, day, emp),
    }
    changes = {f: {"from": old[f], "to": new[f]} for f in TRACKED_FIELDS if old[f] != new[f]}
    entry = {"at": datetime.now(UTC), "by": body.regularized_by, "reason": body.reason, "changes": changes}

    # Optimistic concurrency: only update the record if it still looks exactly as we read it
    # (same punch times, status and number of history entries). A concurrent PATCH or
    # punch-out in between makes this match nothing -> 409, so no history entry is lost.
    guard = {
        "_id": record["_id"],
        "status": old["status"],
        "punch_in": old["punch_in"],    # None also matches a missing field
        "punch_out": old["punch_out"],
        "$expr": {"$eq": [{"$size": {"$ifNull": ["$history", []]}}, len(record.get("history") or [])]},
    }
    # Pipeline-style update so a missing or null `history` becomes [entry]. $literal keeps
    # user text such as a reason starting with "$" from being read as a field path.
    update = [{"$set": {
        **{field: {"$literal": value} for field, value in new.items()},
        "history": {"$concatArrays": [{"$ifNull": ["$history", []]}, [{"$literal": entry}]]},
    }}]
    updated = logs.find_one_and_update(guard, update, return_document=ReturnDocument.AFTER)
    if updated is None:
        raise conflict("the record was changed by another request; reload and retry")
    return record_out(updated)


# --------------------------------------------------------------------------- #
# 10. Aggregation pipeline builders
#
# Each builder returns (collection, pipeline). The analytics endpoints run them and the
# explain endpoint explains the very same pipeline, so the two can never drift apart.
# --------------------------------------------------------------------------- #
LATE = {"$ifNull": ["$late_minutes", 0]}
OVERTIME = {"$ifNull": ["$overtime_minutes", 0]}
IS_PRESENT = {"$in": ["$status", list(PRESENCE_STATUSES)]}
# R5/R6: a present record counts 1, or 0.5 when it is a half day; anything else counts 0.
PRESENT_WEIGHT = {"$cond": [IS_PRESENT, {"$cond": [{"$eq": ["$half_day", True]}, 0.5, 1]}, 0]}
# R7: Mon-Fri. The stored date is a calendar date, so parse it as UTC midnight and take its weekday.
IS_WEEKDAY = {"$lte": [{"$isoDayOfWeek": {"$dateFromString": {"dateString": "$date", "format": "%Y-%m-%d"}}}, 5]}


def count_if(condition) -> dict:
    return {"$sum": {"$cond": [condition, 1, 0]}}


def m_round_half_up(expr, places: int) -> dict:
    """R8 in the database. MongoDB's $round rounds half to even, so do floor(x*10^p + 0.5)/10^p
    in Decimal128 instead (all values here are >= 0). $toDecimal turns a double into a
    15-significant-digit decimal, which also removes binary noise such as 9.124999999999998.
    null stays null."""
    scale = Decimal128(str(10 ** places))
    scaled = {"$add": [{"$multiply": [{"$toDecimal": expr}, scale]}, Decimal128("0.5")]}
    return {"$toDouble": {"$divide": [{"$floor": scaled}, scale]}}


def m_date(expr) -> dict:
    return {"$dateFromString": {"dateString": expr, "format": "%Y-%m-%d"}}


def m_working_days(first_day, last_day: str) -> dict:
    """R7: number of Mon-Fri days from first_day to last_day inclusive (0 if first > last)."""
    days = {"$add": [{"$dateDiff": {"startDate": m_date(first_day), "endDate": m_date(last_day), "unit": "day"}}, 1]}
    return {"$size": {"$filter": {
        "input": {"$range": [0, {"$toInt": {"$max": [days, 0]}}]},
        "as": "i",
        "cond": {"$lte": [{"$isoDayOfWeek": {"$dateAdd": {"startDate": m_date(first_day), "unit": "day", "amount": "$$i"}}}, 5]},
    }}}


def logs_in_range(start: str, end: str, *stages) -> dict:
    """$lookup of one employee's logs between two dates. localField/foreignField plus a
    sub-pipeline uses the uniq_emp_code_date index for both the equality and the range."""
    return {"$lookup": {
        "from": "attendance_logs",
        "localField": "emp_code",
        "foreignField": "emp_code",
        "pipeline": [{"$match": {"date": {"$gte": start, "$lte": end}}}, *stages],
        "as": "logs",
    }}


def pipeline_employee_monthly(emp_code: str, month: str) -> tuple[str, list]:
    start, end = month_bounds(month)
    per_employee = {"$group": {
        "_id": None,
        "present_days": {"$sum": {"$cond": [IS_WEEKDAY, PRESENT_WEIGHT, 0]}},
        "leave_days": count_if({"$eq": ["$status", "LEAVE"]}),
        "late_count": count_if({"$gt": [LATE, 0]}),
        "total_late_minutes": {"$sum": LATE},           # weekend records still count (R7)
        "total_overtime_minutes": {"$sum": OVERTIME},
    }}
    pipeline = [
        {"$match": {"emp_code": emp_code}},
        logs_in_range(start, end, per_employee),
        {"$project": {
            "_id": 0,
            "emp_code": 1,
            "s": {"$ifNull": [{"$first": "$logs"}, {}]},  # no logs at all -> zeros below
            # Mid-month joiners count from joined_on (string max: later date wins).
            "working_days": m_working_days({"$max": ["$joined_on", start]}, end),
        }},
        {"$project": {
            "emp_code": 1,
            "month": {"$literal": month},
            "working_days": 1,
            "present_days": {"$ifNull": ["$s.present_days", 0]},
            "leave_days": {"$ifNull": ["$s.leave_days", 0]},
            "late_count": {"$ifNull": ["$s.late_count", 0]},
            "total_late_minutes": {"$ifNull": ["$s.total_late_minutes", 0]},
            "total_overtime_minutes": {"$ifNull": ["$s.total_overtime_minutes", 0]},
            "attendance_pct": {"$cond": [
                {"$gt": ["$working_days", 0]},
                m_round_half_up({"$multiply": [
                    {"$divide": [{"$toDecimal": {"$ifNull": ["$s.present_days", 0]}}, {"$toDecimal": "$working_days"}]},
                    100]}, 2),
                None,
            ]},
        }},
    ]
    return "employees", pipeline


def pipeline_department_summary(month: str, department: Optional[str]) -> tuple[str, list]:
    start, end = month_bounds(month)
    per_employee = {"$group": {
        "_id": None,
        "present_days": {"$sum": {"$cond": [IS_WEEKDAY, PRESENT_WEIGHT, 0]}},
        # Average over records: keep the sum and the count, divide once per department.
        "work_hours_sum": {"$sum": {"$cond": [
            {"$and": [IS_PRESENT, {"$isNumber": "$work_hours"}]}, {"$toDecimal": "$work_hours"}, 0]}},
        "work_hours_n": count_if({"$and": [IS_PRESENT, {"$isNumber": "$work_hours"}]}),
        "late_count": count_if({"$gt": [LATE, 0]}),
        "total_late_minutes": {"$sum": LATE},
        "leave_count": count_if({"$eq": ["$status", "LEAVE"]}),
        "on_duty_count": count_if({"$eq": ["$status", "ON_DUTY"]}),
    }}
    pipeline = [
        *([{"$match": {"department": department}}] if department is not None else []),
        # Start from employees, not logs, so people with zero logs are still in the headcount.
        # The sort walks the department_emp_code index even when there is no filter.
        {"$sort": {"department": 1, "emp_code": 1}},
        logs_in_range(start, end, per_employee),
        {"$set": {"s": {"$ifNull": [{"$first": "$logs"}, {}]}}},
        {"$group": {
            "_id": "$department",
            "headcount": count_if({"$lte": ["$joined_on", end]}),  # R9: joined by month end
            "present_days": {"$sum": "$s.present_days"},          # $sum skips missing -> 0
            "work_hours_sum": {"$sum": "$s.work_hours_sum"},
            "work_hours_n": {"$sum": "$s.work_hours_n"},
            "late_count": {"$sum": "$s.late_count"},
            "total_late_minutes": {"$sum": "$s.total_late_minutes"},
            "leave_count": {"$sum": "$s.leave_count"},
            "on_duty_count": {"$sum": "$s.on_duty_count"},
        }},
        {"$match": {"headcount": {"$gt": 0}}},
        {"$sort": {"_id": 1}},
        {"$project": {
            "_id": 0,
            "department": "$_id",
            "headcount": 1,
            "present_days": m_round_half_up("$present_days", 2),
            "avg_work_hours": {"$cond": [
                {"$gt": ["$work_hours_n", 0]},
                m_round_half_up({"$divide": ["$work_hours_sum", {"$toDecimal": "$work_hours_n"}]}, 2),
                None,
            ]},
            "late_count": 1,
            "total_late_minutes": 1,
            "leave_count": 1,
            "on_duty_count": 1,
        }},
    ]
    return "employees", pipeline


def pipeline_late_leaderboard(month: str, limit: int, department: Optional[str]) -> tuple[str, list]:
    start, end = month_bounds(month)
    pipeline = [
        {"$match": {"date": {"$gte": start, "$lte": end}, "late_minutes": {"$gt": 0}}},
        {"$group": {"_id": "$emp_code", "total_late_minutes": {"$sum": "$late_minutes"}, "late_count": {"$sum": 1}}},
        {"$lookup": {"from": "employees", "localField": "_id", "foreignField": "emp_code", "as": "emp"}},
        {"$unwind": "$emp"},  # logs whose employee does not exist drop out here
        *([{"$match": {"emp.department": department}}] if department is not None else []),
        # Standard competition ranking (1, 2, 2, 4) computed by the database, not by position.
        {"$setWindowFields": {"sortBy": {"total_late_minutes": -1}, "output": {"rank": {"$rank": {}}}}},
        # limit applies after ranking: everyone tied at a rank <= limit stays in.
        {"$match": {"rank": {"$lte": limit}}},
        {"$sort": {"total_late_minutes": -1, "_id": 1}},
        {"$project": {
            "_id": 0,
            "rank": 1,
            "emp_code": "$_id",
            "name": "$emp.name",
            "department": "$emp.department",
            "total_late_minutes": 1,
            "late_count": 1,
        }},
    ]
    return "attendance_logs", pipeline


def trend_range(from_: str, to: str) -> tuple[date, date]:
    start = date_param(from_, "from")
    end = date_param(to, "to")
    if end < start:
        raise unprocessable("to must not be before from", ("query", "to"))
    if (end - start).days + 1 > TREND_MAX_DAYS:
        raise unprocessable(f"the range may cover at most {TREND_MAX_DAYS} days", ("query", "to"))
    return start, end


def pipeline_department_trend(department: str, start: date, end: date) -> tuple[str, list]:
    """One row per day. Every employee contributes 'events': one per log in the range
    (present / late), plus one 'joined' event on max(joined_on, from). Grouping events by
    day, $densify fills the missing days, and a running $sum of 'joined' is the headcount."""
    first, last = start.isoformat(), end.isoformat()
    pipeline = [
        {"$match": {"department": department}},
        logs_in_range(first, last, {"$project": {
            "_id": 0,
            "date": 1,
            "joined": {"$literal": 0},
            "present": PRESENT_WEIGHT,                    # any day of the week counts here
            "late": {"$cond": [{"$gt": [LATE, 0]}, 1, 0]},
        }}),
        {"$project": {"_id": 0, "events": {"$concatArrays": ["$logs", [
            {   # Clamped into the range so the event never lands outside it.
                "date": {"$min": [{"$max": ["$joined_on", first]}, last]},
                "joined": {"$cond": [{"$lte": ["$joined_on", last]}, 1, 0]},  # R9 for the trend
                "present": {"$literal": 0},
                "late": {"$literal": 0},
            },
            # Zero events on the first and last day: both ends of the range always exist,
            # so $densify only ever has to fill gaps between existing days.
            {"date": first, "joined": {"$literal": 0}, "present": {"$literal": 0}, "late": {"$literal": 0}},
            {"date": last, "joined": {"$literal": 0}, "present": {"$literal": 0}, "late": {"$literal": 0}},
        ]]}}},
        {"$unwind": "$events"},
        {"$replaceWith": "$events"},
        {"$group": {
            "_id": "$date",
            "joined": {"$sum": "$joined"},
            "present_count": {"$sum": "$present"},
            "late_count": {"$sum": "$late"},
        }},
        {"$set": {"day": m_date("$_id")}},
        # Gap filling: one document for every calendar day in [from, to] (upper bound exclusive).
        {"$densify": {"field": "day", "range": {
            "step": 1, "unit": "day",
            "bounds": [datetime.combine(start, time(), UTC), datetime.combine(end + timedelta(days=1), time(), UTC)],
        }}},
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "headcount": {"$sum": "$joined", "window": {"documents": ["unbounded", "current"]}},
        }}},
        {"$set": {
            "is_working_day": {"$lte": [{"$isoDayOfWeek": "$day"}, 5]},
            "present_count": {"$ifNull": ["$present_count", 0]},
            "late_count": {"$ifNull": ["$late_count", 0]},
        }},
        {"$set": {"attendance_rate": {"$cond": [
            {"$and": ["$is_working_day", {"$gt": ["$headcount", 0]}]},
            m_round_half_up({"$divide": [{"$toDecimal": "$present_count"}, {"$toDecimal": "$headcount"}]}, 4),
            None,
        ]}}},
        # The window only ever sees rows inside the range, so the first rows use a shorter
        # window. $avg skips nulls and returns null when nothing is left.
        {"$setWindowFields": {"sortBy": {"day": 1}, "output": {
            "moving_avg_7d": {"$avg": {"$toDecimal": "$attendance_rate"}, "window": {"documents": [-6, "current"]}},
        }}},
        {"$sort": {"day": 1}},
        {"$project": {
            "_id": 0,
            "date": {"$dateToString": {"date": "$day", "format": "%Y-%m-%d"}},
            "is_working_day": 1,
            "headcount": 1,
            "present_count": 1,
            "late_count": 1,
            "attendance_rate": 1,
            "moving_avg_7d": m_round_half_up("$moving_avg_7d", 4),
        }},
    ]
    return "employees", pipeline


def run(collection: str, pipeline: list) -> list:
    return list(db[collection].aggregate(pipeline))


# --------------------------------------------------------------------------- #
# 11. Analytics endpoints
# --------------------------------------------------------------------------- #
MonthParam = Annotated[str, Query(pattern=MONTH_PATTERN, description="YYYY-MM")]


@app.get("/analytics/employees/{emp_code}/monthly")
def employee_monthly(emp_code: str, month: MonthParam):
    rows = run(*pipeline_employee_monthly(emp_code, month))
    if not rows:  # the pipeline starts from the employee, so no row means no employee
        raise not_found(f"employee {emp_code} not found")
    return rows[0]


@app.get("/analytics/departments/summary")
def department_summary(month: MonthParam, department: Optional[str] = None):
    return {"month": month, "items": run(*pipeline_department_summary(month, department))}


@app.get("/analytics/leaderboard/late")
def late_leaderboard(
    month: MonthParam,
    limit: int = Query(10, ge=1, le=50),
    department: Optional[str] = None,
):
    return {"month": month, "items": run(*pipeline_late_leaderboard(month, limit, department))}


@app.get("/analytics/departments/{department}/trend")
def department_trend(
    department: str,
    from_: str = Query(..., alias="from"),
    to: str = Query(...),
):
    start, end = trend_range(from_, to)
    if employees.find_one({"department": department}, {"_id": 1}) is None:
        raise not_found(f"department {department} has no employees")
    return {"department": department, "items": run(*pipeline_department_trend(department, start, end))}


# --------------------------------------------------------------------------- #
# 12. Explain endpoint
# --------------------------------------------------------------------------- #
ExplainTarget = Literal["attendance_list", "employee_monthly", "department_summary", "late_leaderboard", "department_trend"]


def require(value, name: str):
    if value is None:
        raise unprocessable(f"{name} is required for this endpoint", ("query", name))
    return value


def require_month(month: Optional[str]) -> str:
    require(month, "month")
    if not re.match(MONTH_PATTERN, month):
        raise unprocessable("month must be YYYY-MM", ("query", "month"))
    return month


@app.get("/admin/explain/{endpoint}")
def explain_endpoint(
    endpoint: ExplainTarget,
    emp_code: Optional[str] = None,
    month: Optional[str] = None,
    department: Optional[str] = None,
    limit: int = Query(10, ge=1, le=50),
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    status: Optional[Status] = None,
    from_: Optional[str] = Query(None, alias="from"),
    to: Optional[str] = None,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
):
    if endpoint == "attendance_list":
        collection = "attendance_logs"
        command = {
            "find": collection,
            "filter": attendance_list_query(emp_code, date_from, date_to, status),
            "sort": dict(ATTENDANCE_SORT),
            "skip": (page - 1) * page_size,
            "limit": page_size,
        }
    else:
        if endpoint == "employee_monthly":
            collection, pipeline = pipeline_employee_monthly(require(emp_code, "emp_code"), require_month(month))
        elif endpoint == "department_summary":
            collection, pipeline = pipeline_department_summary(require_month(month), department)
        elif endpoint == "late_leaderboard":
            collection, pipeline = pipeline_late_leaderboard(require_month(month), limit, department)
        else:  # department_trend
            start, end = trend_range(require(from_, "from"), require(to, "to"))
            collection, pipeline = pipeline_department_trend(require(department, "department"), start, end)
        command = {"aggregate": collection, "pipeline": pipeline, "cursor": {}}

    result = db.command({"explain": command, "verbosity": "executionStats"})
    # Raw explain output, made JSON-safe (dates, Int64, Timestamps) with relaxed Extended JSON.
    explain = json.loads(json_util.dumps(result, json_options=json_util.RELAXED_JSON_OPTIONS))
    return {"endpoint": endpoint, "collection": collection, "explain": explain}
