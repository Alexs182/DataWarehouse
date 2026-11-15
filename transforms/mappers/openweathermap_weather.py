from typing import Any
from datetime import datetime, timezone

import pandas as pd

from transforms.mappers.map_assist import BaseMapper

class Mapper(BaseMapper):
    entity_type = "WEATHER"

    def records(self, raw):

        for row in raw.to_dict('records'):
            entity_id = row.get('id')

            dt = int(row.get('dt'))
            sunrise = int(row.get('sys.sunrise'))
            sunset = int(row.get('sys.sunset'))

            lat = row.get('coord.lat')
            lon = row.get('coord.lon')

            temp = row.get('main.temp')
            feels_like = row.get('main.feels_like')
            temp_min = row.get('main.temp_min')
            temp_max = row.get('main.temp_max')
            wind_speed = row.get('wind.speed')



            weather = []
            for item in row.get('weather'):
                weather.append({
                    "weather_id": item.get('id'),
                    "weather_name": item.get('main'),
                    "weather_desc": item.get('description')
                })

            entity = {
                "location_id": int(row.get('id')),
                "location_name": row.get('name'),
                "last_updated_at": dt, 
                "timezone": int(row.get('timezone')),
                "location_longitude": float(lon) if pd.notna(lon) else None,
                "location_latitude": float(lat) if pd.notna(lat) else None, 

                "weather": weather,
                "temperature": {
                    "temperature": float(temp) if pd.notna(temp) else None,
                    "feels_like": float(feels_like) if pd.notna(feels_like) else None,
                    "temperature_minimum": float(temp_min) if pd.notna(temp_min) else None,
                    "temperature_maximum": float(temp_max) if pd.notna(temp_max) else None
                },
                "conditions": {
                    "visibility": int(row.get('visibility')),
                    "pressue": int(row.get('main.pressure')),
                    "humidity": int(row.get('main.humidity')),
                    "sea_level": int(row.get('main.sea_level')),
                    "ground_level": int(row.get('main.grnd_level')),
                    "wind_speed": float(wind_speed) if pd.notna(wind_speed) else None,
                    "wind_degrees": int(row.get('wind.deg')),
                    "cloud_cover": int(row.get('clouds.all'))
                },

                "system": {
                    "type": int(row.get('sys.type')),
                    "system_id": int(row.get('id')),
                    "country": row.get('sys.country'),
                    "sunrise_datetime": sunrise,
                    "sunset_datetime": sunset
                }
                
            }

            yield entity_id, entity, datetime.fromtimestamp(dt, timezone.utc).isoformat()


# python3 -m transforms.mappers.openweathermap_weather

if __name__ == "__main__":

    dataframe = pd.DataFrame.from_dict(
        data = [
            {
                "weather": [
                    {'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'},
                    {'id': 800, 'main': 'Clear', 'description': 'clear sky', 'icon': '01d'}
                ],
                "base": "stations",
                "visibility": 10000,
                "dt": 1794672393,
                "timezone": 0,
                "id": 2641324,
                "name": "North Kesteven",
                "cod": 200,
                "coord.lon": -0.6333,
                "coord.lat": 53.1167,
                "main.temp": 279.49,
                "main.feels_like": 276.33,
                "main.temp_min": 278.42,
                "main.temp_max": 280.92, 
                "main.pressure": 1012,
                "main.humidity": 77,
                "main.sea_level": 1012,
                "main.grnd_level": 1009,
                "wind.speed": 4.63,
                "wind.deg": 190,
                "clouds.all": 3,
                "sys.type": 1,
                "sys.id": 1516,
                "sys.country": "GB",
                "sys.sunrise": 1794641074,
                "sys.sunset": 1794672571
            }
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