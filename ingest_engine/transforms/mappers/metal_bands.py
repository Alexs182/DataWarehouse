import sys
import pandas as pd

from transforms.mappers.map_assist import MapAssist

class Mapper(MapAssist):
    def __init__(self):
        self.map_output: pd.DataFrame

        self.entity_type: str = "METAL_BANDS"
        self.event_classifier: str = self.get_date_time().strftime("%Y%m%d%H%M%S")

    def _event_id(self, row):
        return f"{self.entity_type}_{row['band_name']}_{row['formed']}_{self.event_classifier}"
    
    def _entity_id(self, row):
        return f"{self.entity_type}_{row['band_name']}_{row['formed']}"

    def map(self, data):
        dataframe = pd.DataFrame({
            "event_id": data.apply(self._event_id, axis=1),
            "entity_id": data.apply(self._entity_id, axis=1),
            "entity": data.apply(lambda row: {
                "band_name": str(row["band_name"]),
                "fans":      str(row["fans"]),
                "formed":    str(row["formed"]),
                "origin":    str(row["origin"]),
                "split":     str(row["split"]),
                "style":     str(row["style"])
            }, axis=1),
            "event_occurred_timestamp": str(self.get_date_time())
        })

        return dataframe
    