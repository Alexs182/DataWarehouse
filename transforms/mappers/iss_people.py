from transforms.mappers.map_assist import MapAssist

class Mapper(MapAssist):
    def __init__(self):
        self.map_output: list = []

        self.entity_type: str = "ISS_PEOPLE"
        self.event_classifier: str = self.get_date_time().strftime("%Y%m%d%H%M%S")

    def map(self, data):

        for people in data.get('people'):
            
            key = str(people.get('name')).replace(" ", "")

            self.map_output.append({
                "event_id": f"{self.entity_type}_{self.event_classifier}_{key}",
                "entity_id": f"{self.entity_type}_{key}",
                "entity": {
                    "number":           str(data.get('number')),
                    "message":          str(data.get('message')),
                    "name":             str(people.get('name')),
                    "craft":            str(people.get('craft'))
                },
                "event_occurred_timestamp": str(self.get_date_time())
            })

        return self.map_output