# Results

`run.sh` writes the results of a run to `results/<host>-<timestamp>/`. Nothing in this directory
but this file is committed.

| File | What it has |
| --- | --- |
| `results-<durability>.jsonl` | One JSON object per line: a measurement of a case of a workload, on one system (see "Results format" in the README of `bench`) |
| `docker-stats-<durability>.csv` | `docker stats` of the containers every few seconds, each line with the time (UTC) that it was taken at: the CPU and memory of PostgreSQL and of the harness. Join it with the `start` and `end` of the results |
| `postgres-<durability>.log` | The log of the PostgreSQL container |
| `verify.txt` | What `kvbench verify` said, if the run was started with `--verify` |
| `meta.json` | The arguments, the limits, the commit of kvutil and the Docker the run was made on |
| `compose.resolved.yml` | The compose file as it was run, with the variables filled in |
| `images.txt` | The digests of the database images |
