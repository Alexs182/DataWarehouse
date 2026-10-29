
import os
import json
import logging
from typing import Optional, Dict, Any

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError, ArgumentError
from sqlalchemy.types import JSON

from connectors.common import Common

class Connector(Common):

    def __init__(self, logger):
        load_dotenv()
        self.logger = logger

    def check_server(self):
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        except SQLAlchemyError as e:
            self.logger.error(f"Postgres connection check failed: {e}")
            raise
        self.logger.info("Postgres connection check passed.")

    def connect(self, config: Dict[str, Any]):
        connection = config.get("connection", "")
        self.server_url = os.getenv(connection)
        if not self.server_url:
            self.logger.error(f"Connection: {connection} not found in environment variables, check .env file and config.")
            raise Exception("Postgres connection url not found.")

        self.engine = create_engine(self.server_url, pool_pre_ping=True)
        self.check_server()

    def disconnect(self) -> None:
        self.engine.dispose()
        self.logger.info("Postgres Connection closed.")


    def _write_data(self, dataframe, write_mode: str, schema: str, table: str) -> None:

        if write_mode not in ["fail", "replace", "append", "delete_rows"]:
            raise Exception(f"Invalid write mode {write_mode} should be one of: fail, replace, append, delete_rows")

        if 'entity' in dataframe.columns:
            dataframe['entity'] = dataframe['entity'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x )

        try:
            dataframe.to_sql(
                name=table,
                con=self.engine,
                schema=schema,
                if_exists=write_mode,
                index=False,
                chunksize=1000,
                method="multi",
                dtype={'entity': JSON}
            )
        except pd.errors.DatabaseError as e:
            self.logger.error(f"Postgres Write error: {e}")
            raise
        except ArgumentError as e:
            self.logger.error(f"Invalid Postgres server url: {self.server_url}, {e}")
            raise


    def _read_data(self, schema: str, table: str, stage_type: str, sql: str) -> pd.DataFrame:
        dataframe = pd.DataFrame()

        try:
            if sql != "":
                dataframe = pd.read_sql_query(con=self.engine, sql=sql)
            else:
                dataframe = pd.read_sql_table(
                    con=self.engine,
                    table_name=table,
                    schema=schema
                )

        except Exception as e:
            self.logger.error(f"{e}")
            raise
        return dataframe

            
    def run(self, config, dataframe: pd.DataFrame):

        self.connect(config=config)

        schema = config.get("schema", "")
        table = config.get("table", "")

        try:
            match config.get("execution_type", "").lower():
                case "read":
                    dataframe = self._read_data(
                        schema=schema,
                        table=table,
                        stage_type=config.get("stage_type", ""),
                        sql=config.get("sql", "")
                    )
                case "write":
                    self.logger.info("Starting data write")
                    self._write_data(
                        dataframe=dataframe,
                        write_mode=config.get("write_mode", ""),
                        schema=schema,
                        table=table
                    )

                case _:
                    self.logger.error("No valid postgres execution_type in configuration.")
                    raise ValueError("Invalid execution type for Postgres connector, should be either read or write.")

        finally:
            self.disconnect()

        return dataframe       
    
