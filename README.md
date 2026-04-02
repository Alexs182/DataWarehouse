# DataWarehouse

Basic universal ingestion engine and data warehouse management system written in Python. Allows for ingestion pipelines to be defined using simple yaml files. Data is transformed on read into a standardised envelope and inserted into a remote postgres database. 

Data is modelled in a standardised medallion architecture using DBT for consumption within Apache Superset as BI platform. Scheduling and orchestration is achieved using Airflow and monitored using Grafana.

## Technologies
- Docker
- DBT
- Postgres
- Airflow
- Python
- Apache Superset
- Grafana

## Example executions

Pre-reqs:-
- venv is setup and requirements installed
- postgres running and credentials* added to .env file

 ```python

python3 main.py -c config/import/iss_now.yaml -e dev

 ```

## Setting up a new pipeline

### Adding a new config
The config defines the execution of the pipeline, including the source and target systems, credentials, mappings and ingestion type. Existing pipeline definitions can be found in `ingest_engine/config`

The definitions contain two major sections Extract and Target:

```yaml
extract:
  table: "iss_now"
  schema: "staging"
  connection: "data_warehouse"
  mapper: Null
  connector_type: "postgres_connector"
  execution_type: "read"

target:
  file_path: "/mnt/smb_share/alex/datastore/Exports/iss_now.json"
  file_type: "JSON"
  mapper: Null
  connector_type: "file_connector"
  execution_type: "write"
```

Depending on the connector type the config will differ, in the above example data is read from the data_warehouse (in this instance postgres) and written to file. 

### Adding a new mapper
Mappers allow for a customisable definition of data transformation on read. These are defined within the `transforms/mappers` folder. The files are simple python scripts that interpret incoming data and map it to a standardised format for easier downstream consumption.

An example of this transformation system would be:
```python

```

