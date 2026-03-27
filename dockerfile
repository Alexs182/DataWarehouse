FROM apache/airflow:3.1.8

USER root
RUN apt-get update && apt-get install -y git

COPY data_warehouse/ /opt/airflow/data_warehouse/

RUN mkdir -p /opt/airflow/data_warehouse/target && \
    chown -R airflow: /opt/airflow/data_warehouse && \
    chmod -R 775 /opt/airflow/data_warehouse

USER airflow

RUN pip install --no-cache-dir\
    dbt-core \
    dbt-postgres \
    "requests==2.31.0" \
    "urllib3==2.2.3" \
    "charset-normalizer==3.3.2"\
    "chardet==5.2.0" \
    "gcloud-aio-auth"


