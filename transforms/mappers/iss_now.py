from transforms.mappers.map_assist import MapAssist

class Mapper(MapAssist):
    def __init__(self):
        self.map_output: list = []

        self.entity_type: str = "ISS_NOW"
        self.event_classifier: str = self.get_date_time().strftime("%Y%m%d%H%M%S")
        
    def map(self, data):
        key = str(data.get('timestamp'))

        self.map_output.append({
            "event_id": f"{self.entity_type}_{self.event_classifier}_{key}",
            "entity_id": f"{self.entity_type}_{key}",
            "entity": {
                "timestamp":        str(data.get('timestamp')),
                "message":          str(data.get('message')),
                "latitude":         str(data.get('iss_position', {}).get('latitude')),
                "longitude":        str(data.get('iss_position', {}).get('longitude'))
            },
            "event_occurred_timestamp": str(self.get_date_time())
        })

        return self.map_output
    
    def silver_schema(self) -> list:
        return [
            {"name": 'message', "type": "string"},
            {"name": 'timestamp', "type": "string"},
            {"name": 'latitude', "type": "double"},
            {"name": 'longitude', "type": "double"}
        ]