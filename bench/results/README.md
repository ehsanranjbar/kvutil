# Results

`run.sh` writes the results of a run to `results/<host>-<timestamp>/`. Nothing in this directory
but this file is committed.

A run can be added to: `run.sh --out results/<run>` of an existing directory appends to its files
(below) instead of replacing them, so a run can be made in pieces, or a case made again. The
report shows the last of the results of the same durability, system, workload, variant, case and
workers. See "A run made in pieces" in the README of `bench`.

| File | What it has |
| --- | --- |
| `results-<durability>.jsonl` | One JSON object per line: a measurement of a case of a workload, on one system (see "Results format" in the README of `bench`). Appended to by every piece of the run |
| `docker-stats-<durability>.csv` | `docker stats` of the containers every few seconds, each line with the time (UTC) that it was taken at: the CPU and memory of PostgreSQL and of the harness. Join it with the `start` and `end` of the results. Its header is written once, and the pieces of the run are appended |
| `postgres-<durability>.log`, `timescaledb-<durability>.log` | The log of the database container, of each piece one after the other |
| `verify.txt` | What `kvbench verify` said, if the run was started with `--verify` (the last one, if more than one piece had it) |
| `meta.json` | The arguments, the limits, the commit of kvutil and the Docker the run was made on, of the first piece: it is written only if it is not there |
| `compose.resolved.yml` | The compose file as it was run, with the variables filled in |
| `images.txt` | The digests of the database images |
| `report.html`, `report.md`, `charts/` | The report `run.sh` writes at the end (`kvbench report`): one self-contained page, the same as Markdown with its charts as SVG files |
| `*.csv` (but `docker-stats-*`) | The data behind the tables of the report: `runs.csv`, `<run>-<durability>-<workload>.csv` with a line for each result, and the tables of the special sections and of the comparison |
