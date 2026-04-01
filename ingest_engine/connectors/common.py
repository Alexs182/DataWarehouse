
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
        
        self.mapper = mapper

        return mapper
    
    def map_data(
            self, 
            logger,
            records: List[Dict[Any, Any]] | pd.DataFrame
        ) -> List[Dict[Any, Any]] | pd.DataFrame:

        if isinstance(records, list) and not records: 
            logger.warn("Mapping failed: No records to map")
            return []
        elif isinstance(records, pd.DataFrame) and len(records.index) == 0:
            logger.warn("Mapping failed: No records to map")
            return []

        if self.mapper:
            return self.mapper.map(records)
        
        logger.warn("Mapping failed: No mapper to complete mapping")
        return []

    @staticmethod
    def apply_metadata(
            records: pd.DataFrame, 
            source_name: str, 
            source_path: str
        ) -> pd.DataFrame:

        ingest_time = datetime.datetime.now()
        
        records['_ingestion_date'] = ingest_time.strftime("%Y-%m-%d")
        records['_ingestion_timestamp'] = ingest_time.isoformat()
        records['_source_name'] = source_name
        records['_source_path'] = source_path
        records['_raw_json'] = ""

        return records
                
