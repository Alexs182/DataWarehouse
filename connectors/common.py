
import json
import datetime
import importlib
from typing import List, Dict, Any, Optional

import pandas as pd


class Common():
    def __init__(self):
        self.mapper = None

    def get_mapper(self, map: str, logger):
        mapper = None

        try:
            module = importlib.import_module(f'transforms.mappers.{map}')
            mapper = module.Mapper()
            logger.info(f"Loaded logger {map}")
        except ModuleNotFoundError as e:
            logger.warn(f"No mapping module found for {map}. Trace: {e}")
        
        return mapper
    
    def map_data(self, records: List[Dict[str, Any]] | None) -> List[Dict[str, Any]] | None:
        if records:
            return self.mapper(records)

    @staticmethod
    def apply_metadata(
            records: List[Dict[str, Any]] | pd.DataFrame, 
            source_name: str, 
            source_path: str
        ) -> List[Dict[str, Any]] | pd.DataFrame:

        ingest_time = datetime.datetime.now()
        
        # If the source is list of dicts
        if isinstance(records, list):
            for record in records: 
                record['_ingestion_date'] = ingest_time.strftime("%Y-%m-%d")
                record['_ingestion_timestamp'] = ingest_time.isoformat()
                record['_source_name'] = source_name
                record['_source_path'] = source_path
                record['_raw_json'] = json.dumps(record.copy())
        # If the source is a Dataframe.
        else:
            records['_ingestion_date'] = ingest_time.strftime("%Y-%m-%d")
            records['_ingestion_timestamp'] = ingest_time.isoformat()
            records['_source_name'] = source_name
            records['_source_path'] = source_path
            records['_raw_json'] = ""

        return records
                
