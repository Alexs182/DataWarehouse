FROM apache/airflow:3.1.8

USER root
RUN apt-get update && apt-get install -y git

USER airflow

RUN pip install --no-cache-dir\
    dbt-core \
    dbt-postgres \
    "requests==2.31.0" \
    "urllib3==2.2.3" \
    "charset-normalizer==3.3.2"

COPY data_warehouse/ /opt/airflow/data_warehouse/
