import datetime
from typing import Iterable, ClassVar
import sqlalchemy as sa

import pandas as pd
from abc import ABC, abstractmethod

class BaseMapper(ABC):
    entity_type: ClassVar[str]

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if not hasattr(cls, 'entity_type'):
            raise TypeError(f'{cls.__name__} must define entity_type')

    @abstractmethod
    def records(self, raw) -> Iterable[tuple[str, dict, str | None]]:
        '''yield (entity_key, entity_dict, source_timestamp)'''

    def map(self, raw) -> pd.DataFrame:
        now = datetime.datetime.now(datetime.timezone.utc)
        rows = []

        for key, entity, src_ts in self.records(raw):
            stamp = src_ts or now.isoformat()
            rows.append({
                'event_id': f'{self.entity_type}_{key}_{stamp}',
                'entity_id': f'{self.entity_type}_{key}',
                'entity': entity,
                'event_occurred_timestamp': stamp,
            })
        return pd.DataFrame(rows)


class MapAssist():
    def __init__(self):
        pass

    def get_date_time(self):
        return datetime.datetime.now()
    
    def create_table_from_dict(self, data):
        columns = []
        for key, value in data.items():
            if isinstance(value, int):
                col_type = sa.Integer
            else:
                col_type = sa.String(255)
            columns.append(sa.Column(key, col_type))

        print(columns)