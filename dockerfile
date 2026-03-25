FROM apache/airflow:3.1.8

USER root
RUN apt-get update && apt-get install -y git

USER airflow
# Install dbt — swap adapter as needed (bigquery, snowflake, redshift, etc.)
RUN pip install dbt-core dbt-postgres

# Copy the dbt project into the image
COPY data_warehouse/ /opt/airflow/data_warehouse/
