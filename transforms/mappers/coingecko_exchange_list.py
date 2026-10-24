from transforms.mappers.map_assist import MapAssist

class Mapper(MapAssist):
    def __init__(self):
        self.map_output: list = []

        self.entity_type: str = "EXCHANGE_LIST"
        self.event_classifier: str = self.get_date_time().strftime("%Y%m%d%H%M%S")
        
    def map(self, data):
        for item in data:
            self.map_output.append({
                "event_id": f"{self.entity_type}_{self.event_classifier}_{item.get('id')}",
                "entity_id": f"{self.entity_type}_{item.get('id')}",
                "entity": {
                    "id":           str(item.get('id')),
                    "name":         str(item.get('name')),
                },
                "event_occurred_timestamp": str(self.get_date_time())
            })

        return self.map_output