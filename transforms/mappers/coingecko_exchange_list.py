
from datetime import datetime, timezone
from typing import Iterable
import pandas as pd

from transforms.mappers.map_assist import BaseMapper


from transforms.mappers.map_assist import MapAssist


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
