from datetime import datetime, timezone
from transforms.mappers.map_assist import BaseMapper

class Mapper(BaseMapper):
    entity_type = 'ISS_NOW'

    def records(self, raw):
        ts = raw.get('timestamp')
        pos = raw.get('iss_position', {})
        entity = {
            'message': raw.get('message'),
            'latitude': float(pos['latitude']) if pos.get('latitude') else None,
            'longitude': float(pos['longitude']) if pos.get('longitude') else None,
        }
        yield str(ts), entity, datetime.fromtimestamp(ts, timezone.utc).isoformat()
