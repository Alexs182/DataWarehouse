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
                'timestamp': ts,
                'latitude': float(lat) if pd.notna(lat) else None,
                'longitude': float(lon) if pd.notna(lon) else None,
            }
            yield str(ts), entity, datetime.fromtimestamp(ts, timezone.utc).isoformat()


# python3 -m transforms.mappers.iss_now

if __name__ == "__main__":

    dataframe = pd.DataFrame.from_dict(
        data = [
            {"timestamp": 1794671034, "message": "success",  "iss_position.longitude": -72.3065, "iss_position.latitude": -46.2726},
        ]
    )

    print("*"*20)
    print("Input")
    print("*"*20)
    print(dataframe.to_string())

    dataframe = Mapper().map(dataframe)

    print("*"*20)
    print("Output")
    print("*"*20)    
    print(dataframe.to_string())