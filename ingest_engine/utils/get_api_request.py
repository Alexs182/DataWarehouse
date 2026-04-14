
import sys
sys.path.insert(0, '..')

import yaml
import json
import argparse
import logging

import pandas as pd

from connectors.http_connector import Connector

def execute_api_request(config, logger, stage_offset):
    stage = config['stages'][stage_offset]
    mapper = stage['mapper'] 
    if mapper:
        if mapper == "":
            mapper = None


    obj = Connector(
        connection="",
        mapper=mapper,
        logger=logger
    )
    dataframe = obj.run(
        pipeline_config={},
        stage_config=stage,
        dataframe=pd.DataFrame
    )

    raw_response = obj.get_raw_response_data()

    print("RAW DATA: ")
    print(json.dumps(raw_response, indent=4))

    print("MAPPED DATA:")
    print(dataframe)


def get_config(config_file: str):
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

            return config
    except FileNotFoundError:
        raise Exception("Missing config for execution")

def main(args, logger):
    config = get_config(args.config)
    stage_offset = args.stage_offset

    print("CONFIG:")
    print(json.dumps(config, indent=4))

    execute_api_request(config, logger, stage_offset)


def setup_logs():

    logging.basicConfig(
        format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    return logger

def add_args():
    parser = argparse.ArgumentParser(
        description="A simple data ingestion engine",
        epilog="Thanks for using this program!"
    )

    parser.add_argument("-c", "--config", type=str, required=True, help="Path to config file")
    parser.add_argument("-s", "--stage_offset", type=int, required=True, help="The offset of the stage to test")

    args = parser.parse_args()

    return args

if __name__ == "__main__":
    args = add_args()
    logger = setup_logs()
    main(args, logger)