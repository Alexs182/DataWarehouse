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

### 1.6 The OpenWeatherMap mapper loses most of its data

The entity dict defines the key main\_temp eight times (pyflakes flags this). Python keeps only the last, so the stored main\_temp is actually grnd\_level, and temp, feels\_like, temp\_min, temp\_max, pressure, humidity and sea\_level are silently dropped. Rename the keys (main\_temp, main\_feels\_like, main\_temp\_min and so on). Also, data.get('weather', {})\[0\] raises if weather is missing or empty; use a safe first-element helper.

### 1.7 Missing values are stored as the string 'None'

Every mapper wraps values in str(...). A missing field becomes the literal text 'None', and numbers lose their type before they reach the warehouse. Keep native types inside entity (None stays null, numbers stay numbers) and do casting in dbt using the schema you already capture.

### 1.8 Environment-variable checks never fire

os.getenv returns None, not the string 'None'. mongo\_connector and http\_connector compare against 'None', so a missing secret passes straight through. In http\_connector.\_inject\_secret\_values, a key absent from the secrets block makes os.getenv(None) raise a TypeError. The Postgres connector only works because its f-string turns None into 'None'. Use a small helper everywhere:

```python
def require_env(name: str) -> str:
    value = os.getenv(name or '')
    if not value:
        raise RuntimeError(f'Environment variable {name!r} is not set')
    return value
```

### 1.9 The test suite cannot run

python -m unittest tests fails at import. The file stubs the connectors package with an empty module (so from connectors.common import ... fails), and it tests an older Run API (Run(config), \_extract, \_write\_to\_datastore, a target section, SystemExit on failure). See section 6.

### 1.10 Schema capture and dbt generation call a connector API that no longer exists

schema\_transform.\_insert\_schema and dbt\_generator both call Connector.run(pipeline\_config=..., stage\_config=...). The current signature is run(config, dataframe). Details and fixes are in section 5.

## 2. Reliability (P1)

### 2.1 HTTP connector

- **No timeout.** requests.get has none, so a stalled API call can hang an Airflow task or pod indefinitely. Always pass timeout=(5, 30) or similar.
- **method is read but ignored.** Everything is a GET.
- **No retries.** CoinGecko rate-limits aggressively (HTTP 429). Use a Session with urllib3 Retry and backoff.
- **Latent secret leak.** The HTTPError text from requests includes the full URL, query string and all, and you re-wrap it and log it. For OpenWeatherMap that URL contains appid. I found no leaked key in the log you sent, so this is a risk rather than an incident. Scrub or avoid logging URLs with params.
- **Mutating config.** Secrets are injected into the params dict in place; build a copy for the request instead.

```python
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

def make_session() -> requests.Session:
    retry = Retry(total=3, backoff_factor=1.0,
                  status_forcelist=(429, 500, 502, 503, 504),
                  allowed_methods=('GET',))
    s = requests.Session()
    s.mount('https://', HTTPAdapter(max_retries=retry))
    s.mount('http://', HTTPAdapter(max_retries=retry))
    return s
```

### 2.2 Errors are logged and then ignored (DONE)

### 2.3 Mongo connector

 handle BulkWriteError from insert\_many instead of swallowing every exception.

### 2.4 Postgres connector

- You pass a URL string to to\_sql and read\_sql, so pandas builds a fresh engine for every call and never disposes it. Create one engine (pool\_pre\_ping=True) per connector and dispose it at the end.
- write\_mode defaults to an empty string, which pandas rejects. Validate config up front.
- The entity column is written as JSON text via json.dumps. Write it as JSONB with to\_sql(dtype={'entity': JSONB}) and your dbt layer can use native operators.
- psycopg is missing from requirements.txt (see section 7), so writes will fail inside the container.

### 2.5 Time handling

datetime.now() is naive local time. On your dev machine in the UK that shifts by an hour in summer, while a pod runs in UTC, so rows from different environments disagree. Use datetime.now(timezone.utc) everywhere and store real timestamp types. str(datetime) also drops the microseconds when they happen to be zero, which can break the 'YYYY-MM-DD HH24:MI:SS.FF6' parse in the bronze template; isoformat() avoids that.

### 2.6 Deduplication and idempotency

event\_id includes event\_classifier, which is the **run time**. Two overlapping polls that return the same source reading therefore get different event\_ids, so the DEDUPE step in bronze.sql can only ever remove exact re-runs. Build event\_id from source facts instead (entity key plus the source timestamp: ISS timestamp, weather dt, CoinGecko last\_updated\_at) and keep ingestion time in its own column. Similarly, event\_occurred\_timestamp currently holds the ingestion time, not the time the event happened. Either rename it or populate it from the source timestamp.

### 2.7 Raw data is not actually kept

\_raw\_json is always set to an empty string, and the Mongo collections are named iss\_now\_raw\_data and so on, but the Mongo write happens **after** mapping so they hold mapped rows. Write the untouched API response to Mongo before mapping (that is what makes it a raw layer) and either fill \_raw\_json or drop the column.

### 2.8 Validate configs at load time

connector\_type defaults to an empty string, which is how you ended up with 'No module named transforms.' in the logs. Define a small model per stage (pydantic or dataclasses) listing required keys per connector and execution\_type, and fail fast with a clear message. Also call load\_dotenv() once in main.py rather than in every connector constructor.

## 3. Logging (P1)

- **Log to stdout. In k3s the Loki/Promtail path scrapes container stdout. A FileHandler to logs/ is awkward in a container, and in your Dockerfile the logs directory is copied in as root while the process runs as uid 10001, so opening the log file will fail with a permission error. Keep a file handler only as an opt-in via an environment variable.**
- setup\_logs adds a new handler every time it is called; guard against duplicates.
- \_log\_end always reports Records processed: 0. Pass the real count, and add stage name and duration to the structured fields.
- Logger.warn is deprecated; use warning. The message 'Loaded logger {map}' in get\_mapper should say mapper.
- get\_mapper catches ModuleNotFoundError for any missing module, including a broken import inside a mapper file, and reports it as 'no mapping module found'. Check e.name against the mapper module name and re-raise otherwise.
- Replace print() calls in library code (schema\_transform, map\_assist, dbt\_generator) with logger calls.

## 4. Structure and design (P2)

### 4.1 A real mapper contract

Six mappers repeat the same event/entity scaffolding and each has a slightly different shape (some accumulate into self.map\_output, so calling map twice duplicates rows; one returns a DataFrame; two have extra optional methods). A small base class removes the copy-paste and makes mappers pure functions:

```python
class BaseMapper(ABC):
    entity_type: str

    @abstractmethod
    def records(self, raw) -> Iterable[tuple[str, dict, str | None]]:
        '''yield (entity_key, entity_dict, source_timestamp)'''

    def map(self, raw) -> pd.DataFrame:
        now = datetime.now(timezone.utc)
        rows = []
        for key, entity, src_ts in self.records(raw):
            stamp = src_ts or now.isoformat()
            rows.append({
                'event_id': f'{self.entity_type}_{key}_{stamp}',
                'entity_id': f'{self.entity_type}_{key}',
                'entity': entity,
                'event_occurred_timestamp': stamp,
            })
        return pd.DataFrame(rows)
```

Each concrete mapper then only extracts fields, which also makes them trivial to test.

### 4.2 One place for mapping and metadata

file\_connector maps inside \_read\_file, and main.py maps again after the connector returns. For metal\_bands the second pass would fail on the missing band\_name column. Metadata is attached in three places (file\_connector, Common.to\_dataframe, main). Decide that connectors only fetch and write, a transform step maps, and a single function adds metadata.

### 4.3 Connector base class and a registry

Give connectors an abstract base with run(config, dataframe). Replace string-built import paths with a dict (CONNECTORS = {'http\_connector': HttpConnector, ...}). You keep config-driven selection but gain clear errors and easier testing. Run currently inherits Common but never calls Common.**init**; prefer composition over inheriting a grab-bag of helpers.

### 4.4 Remove dead code from the old design

Common.rebuild\_config and mapper.config() (stage-index logic from the earlier multi-stage design), MapAssist.create\_table\_from\_dict, the stage parameter of \_log\_start, and utils/get\_api\_request.py (uses the old signature and dataframe\[0\].to\_json() on a DataFrame).

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

### 7.1 requirements.txt

- **No Postgres driver (psycopg or psycopg2-binary), so the Postgres connector cannot connect in the image.**
- **dotenv==0.9.9 is a compatibility shim; depend on python-dotenv directly.**
- **astronomer-cosmos, apache-airflow-providers-fab and apache-airflow-providers-openlineage belong to your Airflow deployment, not this job image, and two of them are unpinned (and none pulls in apache-airflow itself). They almost certainly explain why the saved image tar is 570 MB.**
- **jinja2 is imported by dbt\_generator but not listed.**
- Split into requirements.txt (runtime) and requirements-dev.txt (pytest, ruff, jinja2, mypy), and pin with a lock file (pip-tools or uv).

### 7.2 Dockerfile

- **COPY logs ./logs ships your local log file into the image and creates a root-owned directory the app user cannot write to. Remove it.**
- **Add a .dockerignore (logs, **pycache**, \*.tar, .env, tests, .git).**
- **Rename dockerfile to Dockerfile.** 
- Tag images with the git SHA rather than only 0.1.0, and push to a registry (a local one on k3s works well) instead of docker save to a tarball.

### 7.3 Repository

The archive contains **pycache** folders, the 570 MB tar, a 38 KB log file, and no .gitignore, .env.example or pyproject.toml.

- **Add .gitignore and a .env.example listing the variables the code expects (default\_mongodb, data\_warehouse, openweathermap\_key).**
- Make it a proper package with **init**.py files and a pyproject.toml, run as python -m ingest\_engine. That removes the sys.path.insert(0, '..') hacks in utils/.
- Update the README: the examples omit the required -s flag, the config format is out of date, and the environment variables are undocumented. Move the notes file into TODO.md or issues.

## 8. Smaller cleanups (P3)

- file\_connector writes 'CSV' with a tab separator (call it TSV or use a comma), writes JSON Lines into files named .json, reads directories with os.walk without filtering by extension, builds its result with pd.concat inside a loop (collect and concat once), and its error message mentions Postgres.
- Add type hints to Run.**init** and replace the dry\_run string with a bool.
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

## What I would keep as is

The YAML-driven design, the importlib-style dynamic connectors (with a registry on top), the event/entity data model, the execution\_id JSON logging, the bronze incremental template with dedupe, and the overall folder layout of connectors, transforms and mappers. These are sound; the work is mostly finishing and hardening rather than redesigning.
