
import json
import datetime
import importlib
from typing import List, Dict, Any, Optional


class Common():
    def __init__(self):
        self.mapper = None

    def get_mapper(self, map):
        module = importlib.import_module(f'transforms.mappers.{map}')
        self.mapper = module.Mapper()
        return self.mapper
    
    def map_data(self, records: List[Dict[str, any]]) -> List[Dict[str, any]]:
            
        return self.mapper(records)

    @staticmethod
    def apply_metadata(
            records: List[Dict[str, Any]], 
            source_name: str, 
            source_path: str
        ) -> List[Dict[str, Any]]:

        ingest_time = datetime.datetime.now()
        
        for record in records: 
            record['_ingestion_date'] = ingest_time.strftime("%Y-%m-%d")
            record['_ingestion_timestamp'] = ingest_time.isoformat()
            record['_source_name'] = source_name
            record['_source_path'] = source_path
            record['_raw_json'] = json.dumps(record.copy())

        return records
                
