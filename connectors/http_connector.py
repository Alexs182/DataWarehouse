
import logging
from typing import Dict, List, Optional, Any

import pandas as pd
import requests
from requests.exceptions import HTTPError

from connectors.common import Common

class Connector(Common):
    def __init__(
            self, 
            mapper: str 
        ):
        self.mapper = self.get_mapper(mapper)
        self.logger = logging.getLogger(__name__)

    def fetch(self, endpoint: str, params: Dict[str, Any], method: str):
        
        try: 
            response = requests.get(f"{endpoint}", params=params)
            response.raise_for_status()
        except HTTPError as e:

            raise HTTPError(f"Failed to read from HTTP source: {e}")

        data = response.json()

        self.logger.info(f"Response Record count: {len(data)}.")


        return data
    
    def map_data(self, records: List[Dict[str, any]]):
        if not records: 
            return None
        
        return self.mapper.map(records)

    def to_dataframe(self, records: List[Dict[str, Any]], source_name: str, source_path: str):
        if not records:
            return pd.DataFrame.from_dict([])
        
        records = self.apply_metadata(records, source_name, source_path)
        df = pd.DataFrame.from_dict(records)

        return df

    def _ingest(
            self,
            endpoint: str,
            source_name: str,
            method: str,
            params: Optional[Dict[str, Any]] = None
        ):
            
        records = self.fetch(endpoint=endpoint, params=params, method=method)
        mapped_records = self.map_data(records=records)
        df = self.to_dataframe(mapped_records, source_name, endpoint)
        
        return df
    
    def run(self, config: Dict[str, Any]):

        dataframe = self._ingest(
            endpoint=config.get('endpoint'),
            method=config.get('method'),
            params=config.get('params'),
            source_name=config.get('source_name')
        )

        return dataframe