# ingest\_engine: Suggestion Report

Code review of the ingest\_engine archive, 23 October 2026

## Summary

The foundation is worth keeping. YAML-driven pipelines, connectors loaded dynamically with importlib, a mapper layer that gives every source the same event/entity shape (event\_id, entity\_id, entity, event\_occurred\_timestamp), ingestion metadata columns, JSON logs with a per-run execution\_id, and a bronze dbt template with incremental loading all point in the right direction for a small warehouse.

The main problem is that the code is **mid-refactor**. main.py moved to an extract/load config format, but only 2 of the 8 pipeline configs were migrated, the load stage is a placeholder, the tests describe an earlier version of the Run class, and the schema/dbt helpers call connector APIs that no longer exist. Most of this report is about finishing that refactor, then hardening it.

**What I did:** read every source file, ran pyflakes, ran the test suite, and ran an old-format config through main.py. **What I did not do:** run anything against live APIs, MongoDB or Postgres. I skipped the 570 MB ingest-0.1.0.tar and the .pyc files.

**Priority key:** P0 = broken now. P1 = will bite in production. P2 = design improvement. P3 = polish.

## 1. Broken right now (P0)
### 1.1 Failed jobs are reported as successful (DONE)
### 1.2 Two config formats, only one supported (DONE)
### 1.3 The load stage is a stub (DONE)
### 1.4 Module-level globals leak into Run (DONE)
### 1.5 Dry run does not do what the flag says (DONE)
### 1.6 The OpenWeatherMap mapper loses most of its data (DONE)
### 1.7 Missing values are stored as the string 'None' (DONE)
### 1.8 Environment-variable checks never fire (DONE)
### 1.9 The test suite cannot run (DONE)
### 1.10 Schema capture and dbt generation call a connector API that no longer exists

schema\_transform.\_insert\_schema and dbt\_generator both call Connector.run(pipeline\_config=..., stage\_config=...). The current signature is run(config, dataframe). Details and fixes are in section 5.

## 2. Reliability (P1) (DONE)
### 2.1 HTTP connector (DONE)
### 2.2 Errors are logged and then ignored (DONE)
### 2.3 Mongo connector (DONE)
### 2.4 Postgres connector (DONE)
### 2.5 Time handling (DONE)
### 2.6 Deduplication and idempotency (DONE)
### 2.7 Raw data is not actually kept (DONE)
### 2.8 Validate configs at load time (DONE)

## 3. Logging (P1)

- setup\_logs adds a new handler every time it is called; guard against duplicates.
- Replace print() calls in library code (schema\_transform, map\_assist, dbt\_generator) with logger calls.

## 4. Structure and design (P2)

### 4.1 A real mapper contract (DONE)
### 4.2 One place for mapping and metadata (DONE)
### 4.3 Connector base class and a registry

- Run currently inherits Common but never calls Common.**init**; prefer composition over inheriting a grab-bag of helpers.

### 4.4 Remove dead code from the old design (DONE)
### 4.5 Config improvements

- **-e is accepted but only written to the logs.** Use it to select per-environment overlays. Today datawarehouse-dev is hard-coded in the YAML, and paths like /mnt/smb\_share/alex/... are hard-coded in three configs.
- The CoinGecko coin list is copy-pasted into four configs. Use YAML anchors for now, and the reference-table approach on your own todo list later.
- Replace the ENVsecret sentinel with explicit interpolation such as ${ENV:OPENWEATHER\_KEY}, resolved once when the config loads. It then works for headers, URLs and connection strings too, which your code comment says you need.
- get\_config exists three times (main, get\_api\_request, dbt\_generator); move it into one module.

## 5. Schema capture and dbt generation (P2)

This is the most interesting part of the project and currently the least working.

- **API mismatch.** \_insert\_schema calls Connector.run(pipeline\_config=\[\], stage\_config=..., dataframe=...). dbt\_generator does the same, unpacks a tuple (dataframe, \_ = ...) from something that returns a single frame, passes the pd.DataFrame class rather than an instance, passes logger=None to a connector that calls self.logger.error, and depends on a ROOT\_DIR global that only exists under **main**.
- **It fetches the oldest schema.** Schema.sql ranks by insert\_datetime ascending and takes row 1. Also insert\_datetime is stored as a date (%Y-%m-%d), so same-day inserts tie. Store a full UTC timestamp and order descending.
- **It appends a row on every run.** Compute a hash of the schema JSON and insert only when it changes. That also gives you free schema-drift detection (log a warning when the hash changes).
- **mapped\_schema is never produced.** The generator filters on schema\_type = 'mapped\_schema', but the only caller, http\_connector, creates raw\_schema. Nothing in main.py currently triggers schema capture after mapping.
- **Schema merge is wrong.** \_get\_schema\_list builds one schema per item and merges with dict(a, \*\*b), so the last item wins. Use a single SchemaBuilder and call add\_object for every record; genson then merges types and tracks required fields properly.
- **Type map bug.** PANDAS\_TYPE\_MAP maps float64 and float32 to 'integer'. Also, if data is neither a list, dict nor DataFrame, schema is never assigned and you hit UnboundLocalError.
- **SQL built with an f-string.** pipeline\_name is interpolated straight into the query. Low risk here, but use bound parameters (sqlalchemy.text with params).
- **The generator ignores types.** bronze.sql emits entity::json->>'field' for every field as text. Once the mappers keep native types (1.7), use the captured types to emit casts such as (entity->>'price')::numeric.
- **Silver templates are empty files**, and iss\_now.silver\_schema() is never read. Either use it or delete it.
- Consider generating a dbt sources.yml and basic tests (unique and not\_null on event\_id) alongside the SQL.

The circular-import workaround (importing the Postgres connector inside a method) is a sign Transform should receive a writer as a constructor argument instead of importing one.

## 6. Testing (P1)

- **Rewrite tests.py** against the current Run, move to a tests/ package, and switch to pytest. Remove the sys.modules stub; it is what breaks imports.
- **Mapper golden-file tests first.** Mappers should be pure, so save a real API response per source under tests/fixtures/ and assert on the output. This is the cheapest, highest-value test you can add, and it would have caught the eight main\_temp keys instantly.
- **Config tests.** Load every YAML under config/ and validate it against your stage model. This one test would have caught the half-migrated configs.
- **Connector tests.** responses or requests-mock for HTTP, mongomock for Mongo, and a throwaway Postgres (testcontainers, or a docker-compose service on your homelab) for the Postgres connector. Your notes already list these as outstanding.
- Add ruff (pyflakes reports around 30 findings today: unused imports, f-strings without placeholders, the duplicate dict keys) and a pre-commit hook so they stay fixed.

## 7. Packaging and repo hygiene (P1)

### 7.1 requirements.txt (DONE)
### 7.2 Dockerfile

- Tag images with the git SHA rather than only 0.1.0, and push to a registry (a local one on k3s works well) instead of docker save to a tarball.

### 7.3 Repository

The archive contains **pycache** folders, the 570 MB tar, a 38 KB log file, and no .gitignore, .env.example or pyproject.toml.

- Make it a proper package with **init**.py files and a pyproject.toml, run as python -m ingest\_engine. That removes the sys.path.insert(0, '..') hacks in utils/.
- Update the README: the examples omit the required -s flag, the config format is out of date, and the environment variables are undocumented. Move the notes file into TODO.md or issues.

## 8. Smaller cleanups (P3)

- file\_connector writes 'CSV' with a tab separator (call it TSV or use a comma), writes JSON Lines into files named .json, reads directories with os.walk without filtering by extension, builds its result with pd.concat inside a loop (collect and concat once), and its error message mentions Postgres.
- Python 3.12 is fine; consider pinning pandas 3.0 behaviours (string dtype is now the default) in tests so dtype-based schema output does not change under you.

## 9. Suggested order of work

| Step | Work | Why this order |
| --- | --- | --- |
| 1 | Fix execute() exit code and logging, remove global leaks (1.1, 1.4) | Small change, stops silent failures immediately |
| 2 | Pick the stage-list config format, migrate all YAMLs, add the config-validation test (1.2, 2.8) | Everything else builds on a working entry point |
| 3 | Implement the stage loop, delete the stub and dead code (1.3, 4.4) | Makes Postgres and Mongo reachable again |
| 4 | BaseMapper, fix the weather mapper, keep native types, source-derived event\_ids, golden-file tests (1.6, 1.7, 2.6, 4.1) | Biggest data-quality gains |
| 5 | HTTP session with timeout and retries, re-raise in connectors, single SQLAlchemy engine (2.1 to 2.4) | Production reliability |
| 6 | Requirements split, Dockerfile, stdout JSON logging (3, 7) | Smaller image, works under k3s and Loki |
| 7 | Repair schema capture and the dbt generator (5) | Depends on steps 3 and 4 |
| 8 | Lint, type hints, README (6, 8) | Polish once behaviour is stable |


