
import os
import json
import logging
from typing import Optional, Dict, Any

from dotenv import load_dotenv
import pandas as pd
import sqlalchemy

from connectors.common import Common

class Connector(Common):

    def __init__(self, connection: str, mapper: Optional[str] = None):
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        self.connection = connection

        if mapper:
            self.mapper = self.get_mapper(mapper)
        else:
            self.mapper = None
        
        self.server_url = f"{os.getenv(self.connection)}"
        if self.server_url == "None":
            self.logger.error(f"Connection: {self.connection} not found in environment variables, check .env file and config.")
            raise Exception("Postgres connection url not found.")

    def _write_data(self, dataframe, write_mode: str, schema: str, table: str):

        dataframe['entity'] = dataframe['entity'].apply(lambda x: json.dumps(x) if isinstance(x, dict) else x )

        try:
            dataframe.to_sql(
                name=table,
                con=self.server_url,
                schema=schema,
                if_exists=write_mode,
                index=False,
                chunksize=100
            )
        except pd.errors.DatabaseError as e:
            self.logger.error(f"Postgres Write error: {e}")
        except sqlalchemy.exc.ArgumentError as e:
            self.logger.error(f"Invalid Postgres server url: {self.server_url}, {e}")


    def _read_data(self, schema: str, table: str):

        dataframe = self.spark.read \
            .format("jdbc") \
            .option("url", self.server_url) \
            .option("dbtable", f"{schema}.{table}") \
            .option("user", self.server_username) \
            .option("password", self.server_password) \
            .option("driver", "org.postgresql.Driver") \
            .load()
        
        if self.mapper:
            dataframe = self.map_data(dataframe)

        return dataframe
            

    def run(self, config: Dict[str, Any], dataframe = None):
        match config.get("execution_type").lower():
            case "read":
                dataframe = self._read_data(
                    config.get("schema"),
                    config.get("table")
                )
                return dataframe
            case "write":
                self._write_data(
                    dataframe,
                    config.get("write_mode"),
                    config.get("schema"),
                    config.get("table")
                )
            case _:
                self.logger.error("No valid postgres execution_type in configuration.")
                raise ValueError("Invalid execution type for Postgres connector, should be either read or write.")

        return None       
    