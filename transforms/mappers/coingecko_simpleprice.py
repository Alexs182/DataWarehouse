
from typing import Any
from datetime import datetime, timezone

import pandas as pd

from transforms.mappers.map_assist import BaseMapper

# Docs:
# https://docs.coingecko.com/demo/reference/simple-price
#
#{
#   "bitcoin": {
#     "usd": 76975,
#     "usd_market_cap": 1542226908349.8406,
#     "usd_24h_vol": 29096603418.89408,
#     "usd_24h_change": -1.4093297098441402,
#     "last_updated_at": 1779092258
#   }
# }

class Mapper(BaseMapper):
    entity_type = "SIMPLE_PRICE"

    def records(self, raw):
        for row in raw.to_dict('records'):
            entity_id = row.get('coin')
            price = row.get('gbp')
            market_cap = row.get('gbp_market_cap')
            volume = row.get('gbp_24h_vol')
            change = row.get('gbp_24h_change')
            last_updated = int(row.get('last_updated_at'))

            entity = {
                'coin_name': entity_id,
                'price': float(price) if pd.notna(price) else None,
                'market_cap': float(market_cap) if pd.notna(market_cap) else None,
                'volume': float(volume) if pd.notna(volume) else None,
                'change': float(change) if pd.notna(change) else None,
                'last_updated': last_updated
            }

            yield str(last_updated), entity, datetime.fromtimestamp(last_updated, timezone.utc).isoformat()

# python3 -m transforms.mappers.coingecko_simpleprice

if __name__ == "__main__":

    dataframe = pd.DataFrame.from_dict(
        data = [
            {"coin": "bitcoin", "gbp": 50081.000000,  "gbp_market_cap": 1.007427e+12, "gbp_24h_vol": 2.437149e+10, "gbp_24h_change": -4.070451, "last_updated_at": 1794666901},
            {"coin": "ethereum", "gbp": 1405.170000,  "gbp_market_cap": 1.717377e+11, "gbp_24h_vol": 8.932079e+09, "gbp_24h_change": -5.225461, "last_updated_at": 1794666906}
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

