

## Testing Configs

```bash
python3 main.py -c config/development/test_1.yaml -e dev
python3 main.py -c config/development/test_2.yaml -e dev
```


```bash
python3 main.py -c config/export/export_staging.yaml -e dev

python3 main.py -c config/import/file/metal_bands.yaml -e dev

python3 main.py -c config/import/api/iss_now.yaml -e dev
python3 main.py -c config/import/api/iss_people.yaml -e dev

python3 main.py -c config/import/api/coingecko_simpleprice.yaml -e dev
python3 main.py -c config/import/api/coingecko_exchange_list.yaml -e dev

```