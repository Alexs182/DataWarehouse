
from datetime import datetime, timezone
from typing import Iterable
import pandas as pd

from transforms.mappers.map_assist import BaseMapper


# Docs:
# https://docs.coingecko.com/demo/reference/exchanges-list
#
#                           id                 name
# 0     10kswap-starknet-alpha              10KSwap
# 1                      9inch                9inch
# 2                     9mm-v3  9mm V3 (Pulsechain)
# 3                9mm-v3-base        9mm V3 (Base)
# 4               9mm-v3-sonic       9mm V3 (Sonic)


class Mapper(BaseMapper):
    entity_type = 'EXCHANGE_LIST'

    def records(self, raw):
        # Raw data contains no natural key and no timestamp so a proxy needs
        # to be included in place
        now = datetime.now(timezone.utc)

        for row in raw.to_dict('records'):
            entity_id = row.get('id')

            entity = {
                'ts': now,
                'id': entity_id,
                'name': row.get('name')
            }

            yield entity_id, entity, now


# python3 -m transforms.mappers.coingecko_exchange_list

if __name__ == "__main__":

    dataframe = pd.DataFrame.from_dict(
        data = [
            {"id": "10kswap-starknet-alpha", "name": "10KSwap"},
            {"id": "9inch", "name": "9inch"},
            {"id": "9mm-v3", "name": "9mm V3 (Pulsechain)"},
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