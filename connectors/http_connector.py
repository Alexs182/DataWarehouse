
import os
import copy
from typing import Dict, List, Optional, Any

from dotenv import load_dotenv
import pandas as pd
import requests
from requests.exceptions import HTTPError

from connectors.common import Common

class Connector(Common):
    def __init__(self, connection: str, mapper: str, logger):
        load_dotenv()
        self.logger = logger
        self.mapper = self.get_mapper(mapper, self.logger) if mapper else None
        
        self.raw_data: List[Dict[Any, Any]] = []

    def get_raw_response_data(self):
        return self.raw_data
    
    def _inject_secret_values(self, key: str, params: Dict[str, Any]) -> Dict[str, Any]:
        key_name = self.config.get("secrets", {}).get(key)
        secret_key = os.getenv(key_name)
        if secret_key == "None":
            self.logger.error(f'Secret: {key} not found in environment variables, check .env file and config.')
            raise Exception("Secret value not found.")

        params[key] = secret_key

        return params

    def fetch(self, endpoint: str, params: Dict[str, Any], method: str):
        
        # Param secrets, this is apparently unsafe and uncommon but required for some
        # of the Endpoints used in this project.
        secret_keys = [k for k, v in params.items() if v == "ENVsecret"]
        for key in secret_keys:
            params = self._inject_secret_values(key, params)

        # To add: same thing but for the headers. Its much more common to need to provide
        # secret values to the headers. Need to add this when required

        try: 
            response = requests.get(f"{endpoint}", params=params)
            response.raise_for_status()
        except HTTPError as e:
            raise HTTPError(f"Failed to read from HTTP source: {e}")

        self.raw_data = response.json()

        self.logger.info(f"Raw Response Record count: {len(self.raw_data)}.")



    def _ingest(self, endpoint: str, method: str, params: Dict[str, Any]):
            
        self.fetch(endpoint=endpoint, params=params, method=method)

        # entry point for raw schema derivation
        if self.config.get('data_schema', {}).get('active'):
            self.get_schema(
                schema_type="raw_schema",
                logger=self.logger,
                records=self.raw_data,
                pipeline_name=self.config.get('job_name', '')
            )

        return self.raw_data
    
    def run(self, config: Dict[str, Any], dataframe: pd.DataFrame):
        self.config = copy.deepcopy(config)

        data = self._ingest(
            endpoint=config.get('endpoint', ''),
            method=config.get('method', ''),
            params=config.get('params', {})
        )

        return data  