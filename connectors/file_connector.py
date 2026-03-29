import os
import json
import logging
from typing import Optional, Dict, Any, List
from pathlib import Path

import pandas as pd

from connectors.common import Common

class Connector(Common):
    def __init__(self, 
            logger,
            mapper: Optional[str] = None
        ):
        self.logger = logger
        
        print(f"Mapper: {mapper}")

        self.mapper = self.get_mapper(mapper, self.logger) if mapper else None

    def _read_data(
            self, 
            file_path: Path,
            file_type: str,
            read_type: str, 
            source_name: str
        ) -> pd.DataFrame:

        file_list = []

        match read_type.lower():
            case "directory":
                for root, dirs, files in os.walk(file_path, topdown=False):
                    for name in files:
                        file_list.append(os.path.join(root, name))
            case "file":
                file_list.append(file_path)
            case _:
                self.logger.error(f"Invalid read_type, should be directory or file")
        
        dataframe = None
        for files in file_list:
            tmp_dataframe = self._read_file(
                file_name=files, 
                file_type=file_type,
                source_name=source_name
            )
            dataframe = pd.concat([dataframe, tmp_dataframe]) if dataframe else tmp_dataframe
        
        return dataframe


    def _read_file(
            self, 
            file_name: str,
            file_type: str,
            source_name: str
        ) -> pd.DataFrame:
        try:
            match file_type.upper():
                case "JSON":
                    dataframe = pd.read_json(file_name)
                case _:
                    self.logger.error(f"Invalid file_type: {file_type}")
        except Exception as e:
            self.logger.error(f"Error reading file: {file_name}. Error: {e}")

        mapped_records = self.map_data(records=dataframe)
        dataframe = self.apply_metadata(
            records=mapped_records, 
            source_name=source_name, 
            source_path=file_name
        )

        return dataframe
        
    def map_data(self, records: List[Dict[Any, Any]]):
        if len(records.index) == 0: 
            self.logger.warn("Mapping failed: No records to map")
            return None
        
        print(self.mapper)
        
        if self.mapper:
            return self.mapper.map(records)
        
        self.logger.warn("Mapping failed: No mapper to complete mapping")

    def run(self, config: Dict[str, Any], dataframe = None):
        match config.get("execution_type", "").lower():
            case "read":
                dataframe = self._read_data(
                    file_path=config.get('file_path'),
                    file_type=config.get('file_type'),
                    read_type=config.get('read_type'),
                    source_name=config.get("source_name")
                )
            case "write":
                self._write_data(

                )
            
            case _:
                self.logger.error("No valid postgres execution_type in configuration.")
                raise ValueError("Invalid execution type for File connector, should be either read or write.")
            
        return dataframe