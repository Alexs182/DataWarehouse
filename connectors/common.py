
import os
import json
import datetime
import importlib
from typing import List, Dict, Any, Optional, Callable, Union
from dataclasses import dataclass

import pandas as pd

from transforms.schema_transform import Transform

Payload = Union[List[Any], Dict[Any, Any]]

@dataclass(frozen=True)
class ShapeConfig:
    """Per-endpoint description of how the JSON response is laid out."""
    shape: str = "records"                  # records | keyed | columnar | rows | envelope
    record_path: Optional[str] = None       # envelope: dotted path to the list, e.g. "data.items"
    meta: Optional[List[str]] = None        # envelope: parent fields to carry onto each row
    columns: Optional[List[str]] = None     # rows: column names for list-of-lists payloads
    id_col: str = "id"                      # keyed: column name for the dict key
    sep: str = "."                          # separator for flattened nested fields

    @classmethod
    def from_dict(cls, cfg: Optional[Dict[str, Any]]) -> "ShapeConfig":
        return cls(**(cfg or {}))

class Common():
    def __init__(self):
        self.mapper = None

    def get_mapper(self, map: str, logger):
        mapper = None

        try:
            module = importlib.import_module(f'transforms.mappers.{map}')
            mapper = module.Mapper()
            logger.info(f"Loaded Mapper {map}")
        except ModuleNotFoundError as e:
            logger.warn(f"No mapping module found for {map}. Trace: {e}")
            raise
        
        self.mapper = mapper

        return mapper
    
    def get_schema(
            self,
            logger,
            schema_type: str,
            pipeline_name: str,
            records: List[Dict[str, Any]] | pd.DataFrame
        ):

        Transform(
            logger=logger
        ).run(  
            data=records,
            schema_type=schema_type,
            pipeline_name=pipeline_name
        )


    def to_dataframe(self, records: Payload, shape_config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        if not records:
            return pd.DataFrame()

        cfg = ShapeConfig.from_dict(shape_config)

        handlers: Dict[str, Callable[[Payload, ShapeConfig], pd.DataFrame]] = {
            "records": self._shape_records,
            "keyed": self._shape_keyed,
            "columnar": self._shape_columnar,
            "rows": self._shape_rows,
            "envelope": self._shape_envelope,
        }

        try:
            handler = handlers[cfg.shape]
        except KeyError:
            raise ValueError(f"Unknown shape '{cfg.shape}'. Expected one of: {sorted(handlers)}")

        return handler(records, cfg)

    @staticmethod
    def _shape_records(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
        if isinstance(payload, dict):
            payload = [payload]
        if not isinstance(payload, list) or not all(isinstance(r, dict) for r in payload):
            raise TypeError("shape 'records' expects a dict or a list of dicts")
        return pd.json_normalize(payload, sep=cfg.sep)

    @staticmethod
    def _shape_keyed(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
        if isinstance(payload, list) and len(payload) == 1:
            payload = payload[0]
        if not isinstance(payload, dict) or not all(isinstance(v, dict) for v in payload.values()):
            raise TypeError("shape 'keyed' expects a dict whose values are all dicts")
        return pd.json_normalize(
            [{cfg.id_col: key, **value} for key, value in payload.items()],
            sep=cfg.sep,
        )

    @staticmethod
    def _shape_columnar(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
        if not isinstance(payload, dict) or not all(isinstance(v, list) for v in payload.values()):
            raise TypeError("shape 'columnar' expects a dict of equal-length lists")
        return pd.DataFrame(payload)

    @staticmethod
    def _shape_rows(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
        if not cfg.columns:
            raise ValueError("shape 'rows' requires 'columns' in the config")
        if not isinstance(payload, list) or not all(isinstance(r, (list, tuple)) for r in payload):
            raise TypeError("shape 'rows' expects a list of lists")
        return pd.DataFrame(payload, columns=cfg.columns)

    @staticmethod
    def _shape_envelope(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
        if not cfg.record_path:
            raise ValueError("shape 'envelope' requires 'record_path' in the config")
        if isinstance(payload, list) and len(payload) == 1:
            payload = payload[0]
        return pd.json_normalize(
            payload,
            record_path=cfg.record_path.split("."),
            meta=cfg.meta,
            sep=cfg.sep,
        )

    def map_data(self, logger, records: List[Dict[Any, Any]] | pd.DataFrame) -> List[Dict[Any, Any]] | pd.DataFrame:

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
    def require_env(name: str) -> str:
        value = os.getenv(name or '')
        if not value:
            raise RuntimeError(f'Environment variable {name!r} is not set')
        return value

    @staticmethod
    def apply_metadata(
            records: pd.DataFrame, 
            source_name: str, 
            source_path: str
        ) -> pd.DataFrame:

        ingest_time = datetime.datetime.now(datetime.timezone.utc)
        
        records['_ingestion_date'] = ingest_time.strftime("%Y-%m-%d")
        records['_ingestion_timestamp'] = ingest_time.isoformat()
        records['_source_name'] = source_name
        records['_source_path'] = source_path
        records['_raw_json'] = ""

        return records
                
@dataclass(frozen=True)
class ShapeConfig:
    """Per-endpoint description of how the JSON response is laid out."""
    shape: str = "records"                  # records | keyed | columnar | rows | envelope
    record_path: Optional[str] = None       # envelope: dotted path to the list, e.g. "data.items"
    meta: Optional[List[str]] = None        # envelope: parent fields to carry onto each row
    columns: Optional[List[str]] = None     # rows: column names for list-of-lists payloads
    id_col: str = "id"                      # keyed: column name for the dict key
    sep: str = "."                          # separator for flattened nested fields

    @classmethod
    def from_dict(cls, cfg: Optional[Dict[str, Any]]) -> "ShapeConfig":
        return cls(**(cfg or {}))

    def to_dataframe(self, records: Payload, shape_config: Optional[Dict[str, Any]] = None) -> pd.DataFrame:
        if not records:
            return pd.DataFrame()

        cfg = ShapeConfig.from_dict(shape_config)

        handlers: Dict[str, Callable[[Payload, ShapeConfig], pd.DataFrame]] = {
            "records": self._shape_records,
            "keyed": self._shape_keyed,
            "columnar": self._shape_columnar,
            "rows": self._shape_rows,
            "envelope": self._shape_envelope,
        }

        try:
            handler = handlers[cfg.shape]
        except KeyError:
            raise ValueError(f"Unknown shape '{cfg.shape}'. Expected one of: {sorted(handlers)}")

        return handler(records, cfg)

@staticmethod
def _shape_records(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
    """[{...}, {...}] or a single {...} -> one row per object, nested fields flattened."""
    if isinstance(payload, dict):
        payload = [payload]
    if not isinstance(payload, list) or not all(isinstance(r, dict) for r in payload):
        raise TypeError("shape 'records' expects a dict or a list of dicts")
    return pd.json_normalize(payload, sep=cfg.sep)


@staticmethod
def _shape_keyed(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
    """{"bitcoin": {...}, "ethereum": {...}} -> one row per key, key stored in cfg.id_col."""
    if isinstance(payload, list) and len(payload) == 1:
        payload = payload[0]  # response wrapped in a single-element list
    if not isinstance(payload, dict) or not all(isinstance(v, dict) for v in payload.values()):
        raise TypeError("shape 'keyed' expects a dict whose values are all dicts")
    return pd.json_normalize(
        [{cfg.id_col: key, **value} for key, value in payload.items()],
        sep=cfg.sep,
    )


@staticmethod
def _shape_columnar(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
    """{"ts": [...], "price": [...]} -> one column per key."""
    if not isinstance(payload, dict) or not all(isinstance(v, list) for v in payload.values()):
        raise TypeError("shape 'columnar' expects a dict of equal-length lists")
    return pd.DataFrame(payload)


@staticmethod
def _shape_rows(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
    """[[ts, o, h, l, c], ...] -> rows, with column names supplied by config."""
    if not cfg.columns:
        raise ValueError("shape 'rows' requires 'columns' in the config")
    if not isinstance(payload, list) or not all(isinstance(r, (list, tuple)) for r in payload):
        raise TypeError("shape 'rows' expects a list of lists")
    return pd.DataFrame(payload, columns=cfg.columns)


@staticmethod
def _shape_envelope(payload: Payload, cfg: ShapeConfig) -> pd.DataFrame:
    """{"results": [...], "next": ...} -> rows from the inner list, optional parent fields as meta."""
    if not cfg.record_path:
        raise ValueError("shape 'envelope' requires 'record_path' in the config")
    if isinstance(payload, list) and len(payload) == 1:
        payload = payload[0]
    return pd.json_normalize(
        payload,
        record_path=cfg.record_path.split("."),
        meta=cfg.meta,
        sep=cfg.sep,
    )