
import os
import json
import logging
from typing import Optional, Dict, Any

from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import exc

from connectors.common import Common

class Connector(Common):

    def __init__(self,      
            connection: str,
            logger: str, 
            mapper: Optional[str] = None
        ):
        load_dotenv()
        self.logger = logger
        self.connection = connection

        self.mapper = self.get_mapper(mapper, self.logger) if mapper else None
        
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
        except exc.ArgumentError as e:
            self.logger.error(f"Invalid Postgres server url: {self.server_url}, {e}")


    def _read_data(self, schema: str, table: str):
        dataframe = pd.DataFrame([])

        try:
            dataframe = pd.read_sql_table(
                con=self.server_url,
                table_name=table,
                schema=schema
            )
        except Exception as e:
            self.logger.error(f"{e}")

        
        if self.mapper:
            dataframe = self.map_data(
                self.logger,
                records=dataframe
            )

        return dataframe
            
    def run(self, 
            pipeline_config, 
            stage_config,
            dataframe: pd.DataFrame
        ):
        match stage_config.get("execution_type", "").lower():
            case "read":
                dataframe = self._read_data(
                    stage_config.get("schema", ""),
                    stage_config.get("table", "")
                )
            case "write":
                self._write_data(
                    dataframe,
                    stage_config.get("write_mode", ""),
                    stage_config.get("schema", ""),
                    stage_config.get("table", "")
                )
            
            case "bypass":
                pass

            case _:
                self.logger.error("No valid postgres execution_type in configuration.")
                raise ValueError("Invalid execution type for Postgres connector, should be either read or write.")

        if stage_config.get("stage_type") == "config":
            pipeline_config = self.rebuild_config(
                dataframe=dataframe,
                pipeline_config=pipeline_config,
                stage_config=stage_config,
                logger=self.logger
            )

        return dataframe, pipeline_config       
    