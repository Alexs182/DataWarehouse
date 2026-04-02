FROM apache/airflow:3.1.8

USER root
RUN apt-get update && apt-get install -y git

COPY data_warehouse/ /opt/airflow/data_warehouse/
COPY ingest_engine/ /opt/airflow/ingest_engine/

RUN mkdir -p /opt/airflow/data_warehouse/target && \
    chown -R airflow: /opt/airflow/data_warehouse && \
    chmod -R 775 /opt/airflow/data_warehouse

RUN chown -R airflow: /opt/airflow/ingest_engine && \
    chmod -R 775 /opt/airflow/ingest_engine

USER airflow

RUN pip install --no-cache-dir\
    dbt-core \
    dbt-postgres \
    "requests==2.31.0" \
    "urllib3==2.2.3" \
    "charset-normalizer==3.3.2"\
    "chardet==5.2.0" \
    "gcloud-aio-auth" \
    "python-json-logger==4.1.0" \
    "PyYAML==6.0.3" \
    "dotenv==0.9.9" \
    "pandas==3.0.1" \
    "SQLAlchemy==2.0.48"
