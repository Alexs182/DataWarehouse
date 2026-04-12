from typing import Any

import pandas as pd

from transforms.mappers.map_assist import MapAssist

class Mapper(MapAssist):
    def __init__(self):
        self.map_output: list = []

        self.entity_type: str = "SIMPLE_PRICE"
        self.event_classifier: str = self.get_date_time().strftime("%Y%m%d%H%M%S")
        
    def map(self, data):
        for key, item in data.items():
            self.map_output.append({
                "event_id": f"{self.entity_type}_{self.event_classifier}_{key}",
                "entity_id": f"{self.entity_type}_{key}",
                "entity": {
                    "coin_name":    str(key),
                    "price":        str(item.get('gbp')),
                    "market_cap":   str(item.get('gbp_market_cap')),
                    "volume":       str(item.get('gbp_24h_vol')),
                    "change":       str(item.get('gbp_24h_change')),
                    "last_updated": str(item.get('last_updated_at'))
                },
                "event_occurred_timestamp": str(self.get_date_time())
            })

        return self.map_output
    
    def config(
            self,
            dataframe: pd.DataFrame,
            pipeline_config: dict[str, Any]
        ):

        coin_list = dataframe['coin_name'].to_list()
        pipeline_config['stages'][1]['params']['ids'] = ",".join(coin_list)

        return pipeline_config