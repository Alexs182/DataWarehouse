
import sys
import os
import io
import json
import datetime
from typing import Dict, List, Optional, Any

import pandas as pd
from genson import SchemaBuilder

#from connectors.common import Common

PANDAS_TYPE_MAP = {
    "str": "string",
    "object": "string",
    "int64": "integer",
    "int32": "integer",
    "float64": "integer",
    "float32": "integer",
    "bool": "boolean",
    "datetime64[ns]": "datetime",
}

class Transform():
    def __init__(
            self,
            logger
        ):
        self.logger = logger

    def _get_schema_list(self, data: List[Dict[str, Any]]):
        builder = SchemaBuilder()
        builder.add_object(data)
        raw = builder.to_schema()

        schema = raw.get('properties', {})

        return schema

    def _get_schema_dataframe(self, dataframe: pd.DataFrame) -> Dict[str, Any]:
        schema = {
            col: {"type": PANDAS_TYPE_MAP.get(str(dtype), str(dtype))}
            for col, dtype in dataframe.dtypes.items()
        }

        if 'entity' in dataframe.columns:
            entity_schema = self._schema_from_entity_column(dataframe)
            schema['entity'] = {
                "type": "object",
                "properties": entity_schema
            }
        
        return schema

    def _schema_from_entity_column(self, df: pd.DataFrame) -> dict:
        builder = SchemaBuilder()
        
        for raw in df['entity'].dropna():
            # Handle both JSON strings and already-parsed dicts
            obj = json.loads(raw) if isinstance(raw, str) else raw
            builder.add_object(obj)
        
        raw_schema = builder.to_schema()
        return raw_schema.get("properties", {})

    def _insert_schema(self, schema: pd.DataFrame):
        stage_config = {
            "stage_type": "data",
            "connector_type": "postgres_connector",
            "execution_type": "write",
            "connection": "data_warehouse",
            "schema": "operational",
            "table": "schemas",
            "write_mode": "append"
        }

        from connectors.postgres_connector import Connector

        Connector(
            connection="data_warehouse",
            logger=self.logger,
            mapper = None
        ).run(
            pipeline_config=[],
            stage_config=stage_config,
            dataframe=schema
        )

    def _generate_dataframe(
            self,
            schema, 
            schema_type,
            pipeline_name
        ) -> pd.DataFrame:

        ingest_time = datetime.datetime.now()

        return pd.DataFrame.from_dict([{
            "pipeline_name": pipeline_name,
            "schema_type": schema_type,
            "insert_datetime": ingest_time.strftime("%Y-%m-%d"),
            "schema": json.dumps(schema)
        }])


    def run(
            self,
            schema_type: str,
            pipeline_name: str,
            data: List[Dict[str, Any]] | pd.DataFrame
        ):

        if isinstance(data, list) or isinstance(data, dict):
            schema = self._get_schema_list(data)
        elif isinstance(data, pd.DataFrame):
            schema = self._get_schema_dataframe(data)
        else:
            print(type(data))

        schema_dataframe = self._generate_dataframe(
            schema=schema, 
            schema_type=schema_type, 
            pipeline_name=pipeline_name
        )

        self._insert_schema(schema_dataframe)

