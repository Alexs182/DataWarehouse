from datetime import datetime, timezone
import pandas as pd

from transforms.mappers.map_assist import BaseMapper


# URL: http://api.open-notify.org/astros.json
# {
#   "people": [
#       {"craft": "ISS", "name": "Oleg Kononenko"}, 
#       {"craft": "ISS", "name": "Nikolai Chub"}, 
#       {"craft": "ISS", "name": "Tracy Caldwell Dyson"}, 
#       {"craft": "ISS", "name": "Matthew Dominick"}, 
#       {"craft": "ISS", "name": "Michael Barratt"}, 
#       {"craft": "ISS", "name": "Jeanette Epps"}, 
#       {"craft": "ISS", "name": "Alexander Grebenkin"}, 
#       {"craft": "ISS", "name": "Butch Wilmore"}, 
#       {"craft": "ISS", "name": "Sunita Williams"}, 
#       {"craft": "Tiangong", "name": "Li Guangsu"}, 
#       {"craft": "Tiangong", "name": "Li Cong"}, 
#       {"craft": "Tiangong", "name": "Ye Guangfu"}
#   ], 
#   "number": 12, 
#   "message": "success"
#}

class Mapper(BaseMapper):
    entity_type = "ISS_PEOPLE"

    def records(self, raw):
        # Raw data contains no natural key and no timestamp so a proxy needs
        # to be included in place
        now = datetime.now(timezone.utc)

        for row in raw.to_dict('records'):
            number = row.get('number')

            entity = {
                'ts': now, 
                'message': row.get('message'),
                'number': float(number) if pd.notna(number) else None,
                'people': row.get('people')
            }

            yield str(now), entity, now


# python3 -m transforms.mappers.iss_people

if __name__ == "__main__":

    dataframe = pd.DataFrame.from_dict(
        data = [
            {
                "people": [
                    {"craft": "ISS", "name": "Oleg Kononenko"},
                    {"craft": "ISS", "name": "Oleg Kononenko"},
                ],
                "number": 2,
                "message": "success"
            },
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