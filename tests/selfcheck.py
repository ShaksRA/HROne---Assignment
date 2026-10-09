"""
Self-check against a REAL MongoDB (6.0+). Uses its own throwaway database and drops it at the end.

    pip install -r requirements.txt httpx
    python tests/selfcheck.py                 # rules, races, analytics vs a plain-Python reference
    python tests/selfcheck.py --big           # also: 100k logs, explain IXSCAN / no COLLSCAN, timings

MONGO_URI is read from the environment / .env as usual. MONGO_DB is forced to `attendance_selfcheck`.
"""
import os
import random
import sys
import threading
import time as clock
from datetime import date, datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
os.environ["MONGO_DB"] = "attendance_selfcheck"

import app.main as m  # noqa: E402
from fastapi import HTTPException  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

UTC, IST = timezone.utc, m.IST
failures = 0


def check(cond, label, extra=""):
    global failures
    print(("  ok   " if cond else "  FAIL ") + label + ("" if cond else f"  {extra}"))
    failures += 0 if cond else 1


def ms(y, mo, d, h, mi, s=0):
    return int(datetime(y, mo, d, h, mi, s, tzinfo=IST).timestamp() * 1000)


def hu(x, p):
    return float(Decimal(str(x)).quantize(Decimal(1).scaleb(-p), ROUND_HALF_UP))


def reset():
    m.client.drop_database(m.db.name)
    m.ensure_indexes()


def parallel(n, fn):
    """Run fn n times at once; return the list of HTTP status codes."""
    codes, barrier = [], threading.Barrier(n)

    def worker():
        barrier.wait()
        try:
            fn()
            codes.append(200)
        except HTTPException as exc:
            codes.append(exc.status_code)

    threads = [threading.Thread(target=worker) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    return codes


# --------------------------------------------------------------------------- #
def test_http_and_races():
    print("HTTP behaviour and races")
    reset()
    with TestClient(m.app) as t:
        check(t.get("/health").status_code == 200, "health")
        emp = {"emp_code": "EMP0001", "name": "A", "email": "a@x.io", "department": "Eng", "joined_on": "2026-01-05"}
        codes = parallel(10, lambda: m.create_employee(m.EmployeeCreate(**{**emp, "emp_code": "EMP0042"})))
        check(sorted(codes) == [200] + [409] * 9, "10 parallel creates -> one 201, nine 409", codes)
        check(t.post("/employees", json=emp).status_code == 201, "create")
        t.post("/employees", json={**emp, "emp_code": "EMP0002", "shift_start": "22:00", "shift_end": "06:00"})

        body = m.PunchInRequest(emp_code="EMP0001", punched_at=ms(2026, 7, 6, 9, 40, 1))
        codes = parallel(20, lambda: m.punch_in(body))
        check(sorted(codes) == [200] + [409] * 19, "20 parallel punch-ins -> one 201, nineteen 409", codes)
        rec = t.get("/attendance", params={"emp_code": "EMP0001"}).json()["items"][0]
        check(rec["late_minutes"] == 10 and rec["date"] == "2026-07-06", "R2 09:40:01 -> 10", rec)

        out = m.PunchOutRequest(emp_code="EMP0001", punched_at=ms(2026, 7, 6, 19, 15))
        codes = parallel(20, lambda: m.punch_out(out))
        check(sorted(codes) == [200] + [409] * 19, "20 parallel punch-outs -> one 200, nineteen 409", codes)

        reqs = [m.RegularizeRequest(punch_in=ms(2026, 7, 6, 9, 0) + i * 60_000, reason=f"fix number {i}", regularized_by="hr")
                for i in range(10)]
        it = iter(reqs)
        lock = threading.Lock()

        def patch():
            with lock:
                req = next(it)
            m.regularize_attendance(req, "EMP0001", "2026-07-06")

        codes = parallel(10, patch)
        hist = t.get("/attendance", params={"emp_code": "EMP0001"}).json()["items"][0]["history"]
        check(codes.count(200) == len(hist) >= 1, "parallel PATCHes: every 200 left exactly one history entry",
              (codes, len(hist)))

        r = t.post("/attendance/punch-in", json={"emp_code": "EMP0002", "punched_at": ms(2026, 7, 7, 5, 0)})
        check(r.status_code == 201 and r.json()["date"] == "2026-07-06", "overnight 05:00 belongs to previous day", r.text)
        r = t.post("/attendance/punch-out", json={"emp_code": "EMP0002", "punched_at": ms(2026, 7, 7, 6, 40)})
        check(r.status_code == 200 and r.json()["overtime_minutes"] == 40, "overnight overtime 40", r.text)

        e = t.get("/admin/explain/attendance_list").json()
        check("explain" in e and e["collection"] == "attendance_logs", "explain endpoint answers", str(e)[:200])


# --------------------------------------------------------------------------- #
def build_dataset(seed=7):
    """Awkward data: ties, no-log employees, joiners before/after the month, weekend and
    legacy records, an overnight employee, and logs of an employee that does not exist."""
    rnd = random.Random(seed)
    depts = ["Engineering", "Sales", "Support", "Ops"]
    emps = []
    for i in range(1, 25):
        emps.append({"emp_code": f"EMP{i:04d}", "name": f"N{i}", "email": f"e{i}@x.io", "department": rnd.choice(depts),
                     "shift_start": "09:30", "shift_end": "18:30",
                     "joined_on": rnd.choice(["2025-11-01", "2026-03-10", "2026-06-17", "2026-07-15", "2026-07-31", "2026-08-03"]),
                     "created_at": datetime(2026, 1, 1, tzinfo=UTC)})
    emps.append({"emp_code": "EMP0099", "name": "Future", "email": "z@x.io", "department": "Future", "shift_start": "22:00",
                 "shift_end": "06:00", "joined_on": "2026-09-01", "created_at": datetime(2026, 1, 1, tzinfo=UTC)})
    logs = []
    for code in [e["emp_code"] for e in emps[:-3]] + ["EMP7777"]:
        d = date(2026, 6, 25)
        while d <= date(2026, 8, 5):
            if rnd.random() < 0.75:
                status = rnd.choices(["PRESENT", "WFH", "ON_DUTY", "ABSENT", "LEAVE"], [6, 2, 1, 1, 1])[0]
                rec = {"emp_code": code, "date": d.isoformat(), "status": status}
                if status in ("ABSENT", "LEAVE"):
                    rec.update(punch_in=None, punch_out=None, work_hours=None, late_minutes=0, overtime_minutes=0, half_day=False)
                else:
                    wh = rnd.choice([None, 3.5, 4.49, 8.12, 8.13, 9.08, 9.0, 8.75, 4.5, 7.005])
                    rec.update(punch_in=datetime(d.year, d.month, d.day, 4, 0, tzinfo=UTC),
                               punch_out=None if wh is None else datetime(d.year, d.month, d.day, 12, tzinfo=UTC),
                               work_hours=wh, late_minutes=rnd.choice([0, 0, 0, 15, 20, 35]),
                               overtime_minutes=rnd.choice([0, 0, 40]), half_day=(wh is not None and wh < 4.5))
                if rnd.random() < 0.15:
                    rec.pop("half_day", None)  # legacy record: no half_day, no history
                else:
                    rec["history"] = []
                logs.append(rec)
            d += timedelta(days=1)
    logs.append({"emp_code": "EMP0099", "date": "2026-07-11", "status": "PRESENT", "punch_in": datetime(2026, 7, 11, 16, tzinfo=UTC),
                 "punch_out": None, "work_hours": None, "late_minutes": 50, "overtime_minutes": 0, "half_day": False, "history": []})
    # A forced tie at the top of July: EMP0001 and EMP0002 get exactly one, identical, late record.
    logs = [r for r in logs if not (r["emp_code"] in ("EMP0001", "EMP0002") and r["date"].startswith("2026-07"))]
    for code in ("EMP0001", "EMP0002"):
        logs.append({"emp_code": code, "date": "2026-07-30", "status": "PRESENT", "punch_in": datetime(2026, 7, 30, 6, tzinfo=UTC),
                     "punch_out": None, "work_hours": None, "late_minutes": 900, "overtime_minutes": 0, "half_day": False, "history": []})
    return emps, logs


def references(emps, logs):
    E = {e["emp_code"]: e for e in emps}

    def weekday(s):
        return date.fromisoformat(s).weekday() < 5

    def weight(r):
        return (0.5 if r.get("half_day") else 1) if r["status"] in m.PRESENCE_STATUSES else 0

    def monthly(code, mo):
        start, end = m.month_bounds(mo)
        first, last = date.fromisoformat(max(E[code]["joined_on"], start)), date.fromisoformat(end)
        wd = sum((first + timedelta(k)).weekday() < 5 for k in range((last - first).days + 1)) if first <= last else 0
        rs = [r for r in logs if r["emp_code"] == code and r["date"].startswith(mo)]
        pres = sum(weight(r) for r in rs if weekday(r["date"]))
        return {"emp_code": code, "month": mo, "working_days": wd, "present_days": pres,
                "leave_days": sum(r["status"] == "LEAVE" for r in rs),
                "late_count": sum((r.get("late_minutes") or 0) > 0 for r in rs),
                "total_late_minutes": sum(r.get("late_minutes") or 0 for r in rs),
                "total_overtime_minutes": sum(r.get("overtime_minutes") or 0 for r in rs),
                "attendance_pct": hu(Decimal(str(pres)) / wd * 100, 2) if wd else None}

    def summary(mo, dept=None):
        start, end = m.month_bounds(mo)
        out = []
        for dp in sorted({e["department"] for e in emps}):
            if dept is not None and dp != dept:
                continue
            es = [e for e in emps if e["department"] == dp]
            hc = sum(e["joined_on"] <= end for e in es)
            if hc == 0:
                continue
            codes = {e["emp_code"] for e in es}
            rs = [r for r in logs if r["emp_code"] in codes and r["date"].startswith(mo)]
            whs = [Decimal(str(r["work_hours"])) for r in rs
                   if r["status"] in m.PRESENCE_STATUSES and r.get("work_hours") is not None]
            out.append({"department": dp, "headcount": hc,
                        "present_days": sum(weight(r) for r in rs if weekday(r["date"])),
                        "avg_work_hours": hu(sum(whs) / len(whs), 2) if whs else None,
                        "late_count": sum((r.get("late_minutes") or 0) > 0 for r in rs),
                        "total_late_minutes": sum(r.get("late_minutes") or 0 for r in rs),
                        "leave_count": sum(r["status"] == "LEAVE" for r in rs),
                        "on_duty_count": sum(r["status"] == "ON_DUTY" for r in rs)})
        return out

    def leaderboard(mo, limit, dept=None):
        totals = {}
        for r in logs:
            if r["date"].startswith(mo) and (r.get("late_minutes") or 0) > 0 and r["emp_code"] in E:
                if dept is not None and E[r["emp_code"]]["department"] != dept:
                    continue
                t = totals.setdefault(r["emp_code"], [0, 0])
                t[0] += r["late_minutes"]
                t[1] += 1
        rows = sorted(totals.items(), key=lambda kv: (-kv[1][0], kv[0]))
        out = []
        for code, (total, n) in rows:
            rank = 1 + sum(1 for _, (t2, _) in rows if t2 > total)
            if rank <= limit:
                out.append({"rank": rank, "emp_code": code, "name": E[code]["name"], "department": E[code]["department"],
                            "total_late_minutes": total, "late_count": n})
        return out

    def trend(dept, a, b):
        es = [e for e in emps if e["department"] == dept]
        codes = {e["emp_code"] for e in es}
        out, rates = [], []
        for k in range((b - a).days + 1):
            d = a + timedelta(k)
            s = d.isoformat()
            hc = sum(e["joined_on"] <= s for e in es)
            rs = [r for r in logs if r["emp_code"] in codes and r["date"] == s]
            pc = sum(weight(r) for r in rs)
            rate = hu(Decimal(str(pc)) / hc, 4) if d.weekday() < 5 and hc > 0 else None
            rates.append(rate)
            window = [x for x in rates[-7:] if x is not None]
            out.append({"date": s, "is_working_day": d.weekday() < 5, "headcount": hc, "present_count": pc,
                        "late_count": sum((r.get("late_minutes") or 0) > 0 for r in rs), "attendance_rate": rate,
                        "moving_avg_7d": hu(sum(Decimal(str(x)) for x in window) / len(window), 4) if window else None})
        return out

    return monthly, summary, leaderboard, trend


def test_analytics():
    print("Analytics against a plain-Python reference")
    reset()
    emps, logs = build_dataset()
    m.employees.insert_many([dict(e) for e in emps])
    m.logs.insert_many([dict(r) for r in logs])
    monthly, summary, leaderboard, trend = references(emps, logs)
    depts = sorted({e["department"] for e in emps}) + ["Nope"]
    mismatches = []

    def same(label, got, expected):
        if got != expected:
            mismatches.append(label)
            print(f"    MISMATCH {label}\n      got      {got}\n      expected {expected}")
    with TestClient(m.app) as t:
        for mo in ("2026-06", "2026-07", "2026-08"):
            for e in emps:
                got = t.get(f"/analytics/employees/{e['emp_code']}/monthly", params={"month": mo}).json()
                same(f"monthly {e['emp_code']} {mo}", got, monthly(e["emp_code"], mo))
            for dp in [None] + depts:
                params = {"month": mo, **({"department": dp} if dp else {})}
                got = t.get("/analytics/departments/summary", params=params).json()["items"]
                same(f"summary {mo} {dp}", got, summary(mo, dp))
                for lim in (1, 2, 3, 5, 50):
                    got = t.get("/analytics/leaderboard/late", params={**params, "limit": lim}).json()["items"]
                    same(f"leaderboard {mo} {dp} limit={lim}", got, leaderboard(mo, lim, dp))
        for dp in depts[:-1]:
            for a, b in [(date(2026, 7, 1), date(2026, 7, 31)), (date(2026, 6, 20), date(2026, 8, 10)), (date(2026, 7, 11), date(2026, 7, 11))]:
                got = t.get(f"/analytics/departments/{dp}/trend", params={"from": a.isoformat(), "to": b.isoformat()}).json()["items"]
                same(f"trend {dp} {a}..{b}", got, trend(dp, a, b))
        check(t.get("/analytics/employees/EMP4040/monthly", params={"month": "2026-07"}).status_code == 404, "monthly 404")
        check(t.get("/analytics/departments/Nope/trend", params={"from": "2026-07-01", "to": "2026-07-02"}).status_code == 404, "trend 404")
        top = t.get("/analytics/leaderboard/late", params={"month": "2026-07", "limit": 1}).json()["items"]
        check([(r["rank"], r["emp_code"]) for r in top] == [(1, "EMP0001"), (1, "EMP0002")], "limit=1 with a tie returns both rank-1 rows", top)
    check(not mismatches, "all analytics match the reference", f"{len(mismatches)} mismatches")


# --------------------------------------------------------------------------- #
def test_big():
    print("100k records: explain plans and timings")
    reset()
    rnd = random.Random(1)
    depts = [f"Dept{i}" for i in range(8)]
    emps = [{"emp_code": f"EMP{i:05d}", "name": f"N{i}", "email": f"e{i}@x.io", "department": rnd.choice(depts),
             "shift_start": "09:30", "shift_end": "18:30", "joined_on": "2025-01-01", "created_at": datetime(2025, 1, 1, tzinfo=UTC)}
            for i in range(400)]
    m.employees.insert_many(emps)
    batch, day0 = [], date(2025, 9, 1)
    for e in emps:
        for k in range(250):
            d = day0 + timedelta(k)
            batch.append({"emp_code": e["emp_code"], "date": d.isoformat(), "status": rnd.choice(["PRESENT", "WFH", "LEAVE"]),
                          "punch_in": datetime(d.year, d.month, d.day, 4, tzinfo=UTC), "punch_out": None, "work_hours": 8.5,
                          "late_minutes": rnd.choice([0, 0, 12, 30]), "overtime_minutes": 0, "half_day": False, "history": []})
    m.logs.insert_many(batch)
    m.ensure_indexes()
    with TestClient(m.app) as t:
        cases = [
            ("attendance_list", {}), ("attendance_list", {"status": "WFH"}),
            ("attendance_list", {"emp_code": "EMP00007", "date_from": "2026-01-01", "date_to": "2026-02-01"}),
            ("attendance_list", {"date_from": "2026-01-01", "page": 3}),
            ("employee_monthly", {"emp_code": "EMP00007", "month": "2026-01"}),
            ("department_summary", {"month": "2026-01"}), ("department_summary", {"month": "2026-01", "department": "Dept1"}),
            ("late_leaderboard", {"month": "2026-01"}), ("late_leaderboard", {"month": "2026-01", "department": "Dept1"}),
            ("department_trend", {"department": "Dept1", "from": "2026-01-01", "to": "2026-03-31"}),
        ]
        for endpoint, params in cases:
            body = t.get(f"/admin/explain/{endpoint}", params=params).text
            check("IXSCAN" in body and "COLLSCAN" not in body, f"explain {endpoint} {params}: IXSCAN, no COLLSCAN")
        for path, params in [("/attendance", {}), ("/attendance", {"status": "LEAVE", "page": 50}),
                             ("/analytics/employees/EMP00007/monthly", {"month": "2026-01"}),
                             ("/analytics/departments/summary", {"month": "2026-01"}),
                             ("/analytics/leaderboard/late", {"month": "2026-01"}),
                             ("/analytics/departments/Dept1/trend", {"from": "2026-01-01", "to": "2026-03-31"})]:
            start = clock.perf_counter()
            r = t.get(path, params=params)
            took = (clock.perf_counter() - start) * 1000
            check(r.status_code == 200 and took < 1500, f"{path} {params}: {took:.0f} ms")


if __name__ == "__main__":
    try:
        test_http_and_races()
        test_analytics()
        if "--big" in sys.argv:
            test_big()
    finally:
        m.client.drop_database(m.db.name)
    print("\nALL CHECKS PASSED" if failures == 0 else f"\n{failures} CHECK(S) FAILED")
    sys.exit(1 if failures else 0)
