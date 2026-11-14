from typing import Any
from datetime import datetime, timezone

import pandas as pd

from transforms.mappers.map_assist import BaseMapper

class Mapper(MapAssist):
    def __init__(self):
        self.map_output: list = []

        self.entity_type: str = "WEATHER"
        self.event_classifier: str = self.get_date_time().strftime("%Y%m%d%H%M%S")
        
    def map(self, data):
        key = str(data.get('id'))

        self.map_output.append({
            "event_id": f"{self.entity_type}_{self.event_classifier}_{key}",
            "entity_id": f"{self.entity_type}_{key}",
            "entity": {
                "id":               str(data.get('id')),
                "name":             str(data.get('name')),
                "timezone":         str(data.get('timezone')),
                "cod":              str(data.get('cod')),
                "datetime":         str(data.get('dt')),
                "base":             str(data.get('base')),   

                "latitude":         str(data.get('coord', {}).get('lat')),
                "longitude":        str(data.get('coord', {}).get('lon')),                                   

                "weather_id":         str(data.get('weather', {})[0].get('id')),
                "weather_main":       str(data.get('weather', {})[0].get('main')), 
                "weather_desc":       str(data.get('weather', {})[0].get('description')),
                "weather_icon":       str(data.get('weather', {})[0].get('icon')), 

                "main_temp":        str(data.get('main', {}).get('temp')),
                "main_temp":        str(data.get('main', {}).get('feels_like')),
                "main_temp":        str(data.get('main', {}).get('temp_min')),
                "main_temp":        str(data.get('main', {}).get('temp_max')),
                "main_temp":        str(data.get('main', {}).get('pressure')),
                "main_temp":        str(data.get('main', {}).get('humidity')),
                "main_temp":        str(data.get('main', {}).get('sea_level')),
                "main_temp":        str(data.get('main', {}).get('grnd_level')),

                "visibility":       str(data.get('visibility')),

                "wind_speed":       str(data.get('wind', {}).get('speed')),
                "wind_deg":         str(data.get('wind', {}).get('deg')),

                "clouds_all":       str(data.get('clouds', {}).get('all')),

                "sys_type":         str(data.get('sys', {}).get('type')),
                "sys_id":           str(data.get('sys', {}).get('id')),
                "sys_country":      str(data.get('sys', {}).get('country')),
                "sys_sunrise":      str(data.get('sys', {}).get('sunrise')),
                "sys_sunset":       str(data.get('sys', {}).get('sunset'))
            },
            "event_occurred_timestamp": str(self.get_date_time())
        })

        return self.map_output

class Mapper(BaseMapper):
    entity_type = "WEATHER"

    def records(self, raw):

        for row in raw.to_dict('records'):
            entity_id = row.get('id')

            entity = {
                
            }

            yield entity_id, entity, 


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