# DECISIONS.md

1. **Indexes.**
   * `employees {emp_code}`, unique: duplicate creates get 409; every lookup by code.
   * `employees {department, emp_code}`: department filter and sort, summary, trend.
   * `attendance_logs {emp_code, date}`, unique: one record per day; punch-in races, punch-out, PATCH, per-employee `$lookup`s.
   * `{date desc, emp_code}`: list sort and month scans.
   * `{status, date desc, emp_code}`: status filters.
   * Rejected `{emp_code, punch_in}` for punch-out: walking `{emp_code, date}` newest-first finds the record in one or two keys.

2. **Punch-in race.** Both requests compute the same `(emp_code, date)` and call `insert_one` without reading first. The unique index admits one insert; the other gets `DuplicateKeyError`, which becomes 409.

3. **Ties.** `$setWindowFields` with `$rank` gives 1, 2, 2, 4. I filter `rank <= limit` afterwards, so everyone tied at the cutoff is returned, ordered by `emp_code`. The response can exceed `limit`.

4. **Headcount.** The pipeline starts from `employees`, not logs, and joins each employee's month of logs with `$lookup`. Someone with no logs still produces a row, so they still count toward headcount if `joined_on` ≤ month end.

5. **At 100x.** I'd precompute daily per-department rollups when a record is written, so the trend and summary read a few hundred small documents instead of joining raw logs.
