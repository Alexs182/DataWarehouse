#!/bin/bash

cd /home/alexs/Code/DataWarehouse/

source .venv/bin/activate

python3 main.py -c config/iss_now.yaml -e dev
python3 main.py -c config/coingecko_simpleprice.yaml -e dev