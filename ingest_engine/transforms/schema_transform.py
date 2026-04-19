
import sys
import os
import io
from typing import Dict, List, Optional, Any

import pandas as pd

from connectors.common import Common
from connectors.postgres_connector import Connector

class Transform(Common):
    def __init__(
            self,
            logger
        ):
        self.logger = logger

    def _get_schema_list(self, data: List[Dict[str, Any]]):
        

    def _get_schema(self, dataframe: pd.DataFrame) -> pd.DataFrame:
        buffer = io.StringIO()
        dataframe.info(buf=buffer)
        schema = buffer.getvalue()

        print(schema)

        sys.exit(1)
        
        return dataframe

    def _insert_schema(self, schema: pd.DataFrame):
        stage_config = {
            "stage_type": "data",
            "connector_type": "postgres_connector",
            "execution_type": "write",
            "connection": "operational_datastore",
            "schema": "operational",
            "table": "schemas",
            "write_mode": "append"
        }

        Connector(
            connection="operational_datastore",
            logger=self.logger,
            mapper = self.mapper
        ).run(
            pipeline_config=[],
            stage_config=stage_config,
            dataframe=schema
        )

    def run(
            self,
            data: List[Dict[str, Any]] | pd.DataFrame
        ):

        if isinstance(data, list):
            schema = self._get_schema_list(data)

        schema = self._get_schema(dataframe)
        self._insert_schema(schema)

