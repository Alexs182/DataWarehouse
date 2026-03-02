import datetime
import sqlalchemy as sa

class MapAssist():
    def __init__(self):
        pass

    def get_date_time(self):
        return datetime.datetime.now()
    
    def create_table_from_dict(self, data):
        columns = []
        for key, value in data.items():
            if isinstance(value, int):
                col_type = sa.Integer
            else:
                col_type = sa.String(255)
            columns.append(sa.Column(key, col_type))

        print(columns)