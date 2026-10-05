

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


## Schema generation
There is a need to scrape and store the schemas of incoming pipelines. The aim of this is to aid 
in the development of new pipelines and cut down on the current manual intervention needed to build
a new pipeline. 

The schemas are all stored within a table on the operational schema of the data warehouse database.

### Workflow

1. SQL section defined in config
2. If its active extract the schema from both the source and the mapped data
3. Parse the schema and store it in the table 

The parsed schema can then be used by the: 
`utils/dbt_generator`
To build out the templates using the data stored in the warehouse. The generated sql is stored in 
the:
`utils/templates/generated` 
folder for manual review and movement into the models folder for DBT.



