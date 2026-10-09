# Employee Attendance & Analytics API

FastAPI + MongoDB (6.0+). All application code is in `app/main.py`.

## Run

```bash
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt
export MONGO_URI="mongodb://localhost:27017" MONGO_DB="attendance_db"   # or put them in a local .env
uvicorn app.main:app --port 8000
```

* `MONGO_URI` / `MONGO_DB` come from the environment. A `.env` file is loaded, but real environment variables win (see `.env.example`).
* Indexes are created at startup, idempotently. There are no manual steps.
* `GET /health` returns 200 only when MongoDB answers a ping, otherwise 503.

## Self-check (optional)

`tests/selfcheck.py` runs against a real MongoDB in a throwaway database (`attendance_selfcheck`), which it drops at the end. It checks:

* the R1-R5 rules and status codes
* parallel creates, punch-ins, punch-outs and PATCHes
* every analytics endpoint against a plain-Python reference, on awkward data (ties, no-log employees, mid-month and future joiners, weekend and legacy records, orphan logs)

With `--big`, it also loads ~100k records and checks every explain plan for IXSCAN and no COLLSCAN.

```bash
pip install httpx
python tests/selfcheck.py --big
```

## Interpretations worth knowing

* **Trend range:** at most 92 days *inclusive* (e.g. 1 Jul to 30 Sep is allowed; one more day is 422).
* **Punch-out earlier than the open punch-in:** if the employee has an open record that started after `punched_at`, the answer is 422 ("punched_at must be after punch_in"), not 404/409. The 404 is for no punch-in at all; the 409 is for an already closed record.
* **Headcount vs. records:** headcount follows R9 (`joined_on` ≤ end of period). Records are counted for every employee of the department, as stored; they are not filtered by `joined_on`.
* **Moving average:** the mean of the *reported* (already rounded) `attendance_rate` values in the window.
* **Rounding:** all R8 rounding is half-up, done in Decimal128 inside the pipelines. MongoDB's `$round` rounds half to even, so it isn't used.
* **PATCH:** derived fields sent by a client are ignored (contract convention). A PATCH that changes none of `status` / `punch_in` / `punch_out` is 422. A concurrent change to the same record makes the loser get 409; history is never lost.
* **Instants:** stored as BSON dates, returned as epoch ms. The client is `tz_aware`, so every datetime in the code is aware UTC and converted to IST only for R1-R3.
