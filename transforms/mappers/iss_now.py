from datetime import datetime, timezone
import pandas as pd

from transforms.mappers.map_assist import BaseMapper

class Mapper(BaseMapper):
    entity_type = 'ISS_NOW'

    def records(self, raw):
        for row in raw.to_dict('records'):
            ts = int(row['timestamp'])
            lat = row.get('iss_position.latitude')
            lon = row.get('iss_position.longitude')
            entity = {
                'message': row.get('message'),
                'latitude': float(lat) if pd.notna(lat) else None,
                'longitude': float(lon) if pd.notna(lon) else None,
            }
            yield str(ts), entity, datetime.fromtimestamp(ts, timezone.utc).isoformat()
