from typing import Any

import pandas as pd

from transforms.mappers.map_assist import MapAssist

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