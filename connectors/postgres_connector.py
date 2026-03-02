
import os
from typing import Optional, Dict, Any

from dotenv import load_dotenv
import pandas as pd

from connectors.common import Common

class Connector(Common):

    def __init__(
            self, 
            mapper: Optional[str] = None
        ):

        load_dotenv()

        if mapper:
            self.mapper = self.get_mapper(mapper)
        else:
            self.mapper = None

        self.server_url: str = None

    def _configure(
            self,
            connection: str,
            database: str
        ):
        
        self.server_url = f"{os.getenv(connection)}/{database}"

    def _build_schema(self):
        pass

    def _write_data(
            self,
            dataframe,
            write_mode: str,
            schema: str,
            table: str
        ):

        dataframe.to_sql(
            name=table,
            con=self.server_url,
            schema=schema,
            if_exists=write_mode,
            index=False,
            chunksize=100
        )

    def _read_data(
            self,
            schema: str,
            table: str
        ):

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
            

    def run(
            self,
            config: Dict[str, Any],
            dataframe = None,
        ):

        self._configure(
            connection=config.get("connection"),
            database=config.get("database")
        )

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
                raise ValueError("Invalid exeuction type for Postgres connector, should be either read or write.")

        return None       
    