
import os
from typing import Dict, List, Optional, Any

from dotenv import load_dotenv
import pandas as pd
import requests
from requests.exceptions import HTTPError

from connectors.common import Common

class Connector(Common):
    def __init__(
            self,
            connection: str, 
            mapper: str,
            logger
        ):
        load_dotenv()
        self.logger = logger
        self.mapper = self.get_mapper(mapper, self.logger) if mapper else None
        
        self.raw_data: List[Dict[Any, Any]] = []

    def get_raw_response_data(self):
        return self.raw_data
    
    def _inject_secret_values(self, key: str, params: Dict[str, Any]) -> Dict[str, Any]:
        print(key)
        key_name = self.stage_config.get("secrets").get(key)
        print(key_name)
        secret_key = os.getenv(key_name)
        if secret_key == "None":
            self.logger.error(f'Secret: {key} not found in environment variables, check .env file and config.')
            raise Exception("Secret value not found.")

        params[key] = secret_key

        print(params)

        return params

    def fetch(self, endpoint: str, params: Dict[str, Any] | None, method: str):
        
        secret_keys = [k for k, v in params.items() if v == "ENVsecret"]
        for key in secret_keys:
            params = self._inject_secret_values(key, params)

        try: 
            response = requests.get(f"{endpoint}", params=params)
            response.raise_for_status()
        except HTTPError as e:
            raise HTTPError(f"Failed to read from HTTP source: {e}")

        self.raw_data = response.json()

        self.logger.info(f"Raw Response Record count: {len(self.raw_data)}.")

    def to_dataframe(self, records: List[Dict[Any, Any]], source_name: str, source_path: str) -> pd.DataFrame:
        if not records:
            return pd.DataFrame([])
        
        dataframe = pd.DataFrame(records)
        dataframe = self.apply_metadata(dataframe, source_name, source_path)
        
        return dataframe

    def _ingest(
            self,
            endpoint: str,
            source_name: str,
            method: str,
            params: Optional[Dict[str, Any]] = {}
        ):
            
        self.fetch(endpoint=endpoint, params=params, method=method)

        mapped_records = self.map_data(
            logger=self.logger, 
            records=self.raw_data
        )

        if isinstance(mapped_records, list):
            df = self.to_dataframe(
                records = mapped_records, 
                source_name = source_name, 
                source_path = endpoint
            )
        
        return df
    
    def run(self, 
            pipeline_config: Dict[str, Any],
            stage_config: Dict[str, Any],
            dataframe: pd.DataFrame
        ):

        self.stage_config = stage_config

        dataframe = self._ingest(
            endpoint=stage_config.get('endpoint', ''),
            method=stage_config.get('method', ''),
            params=stage_config.get('params', ''),
            source_name=stage_config.get('source_name', '')
        )
        
        if stage_config.get("stage_type") == "config":
            pipeline_config = self.rebuild_config(
                dataframe=dataframe,
                pipeline_config=pipeline_config,
                stage_config=stage_config,
                logger=self.logger
            )

        return dataframe, pipeline_config       