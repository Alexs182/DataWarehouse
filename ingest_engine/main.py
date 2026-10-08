
import sys
import yaml
import copy
import uuid
import argparse
import logging
import importlib
from typing import Any

from pythonjsonlogger.json import JsonFormatter
import pandas as pd

from connectors.common import Common

class Run:
    def __init__(self, config, stage_type: str, dry_run: str):
        self.pipeline_config = config
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)
        self.job_name = self.pipeline_config['job_name']
        self.stage_type = stage_type

        self.dataframe = pd.DataFrame()

        self._log_start(self.pipeline_config)

        self.execution_index = 0

    def execute(self):
        config = self.pipeline_config[self.stage_type]

        try:
            logger.info(f'Starting Execution: {self.stage_type} of pipeline {self.job_name}')

            config = self._execute_stage(
                config=copy.deepcopy(config),
                stage_type=self.stage_type
            )

        except Exception as e:
            self.logger.error(f"Job failed: {e}", exc_info=True)
            self._log_end(success=False)
        
        self._log_end(success=True)

    def _execute_stage(self, config, stage_type: str) -> dict[str, Any]:

        match stage_type:
            case "extract":
                # Load the connector module
                connector_module = self._get_module("connectors", config.get('connector_type', ''))
                # Execute the loader process
                self.dataframe = connector_module.Connector(
                    mapper=config.get("mapper"),
                    connection=config.get('connection'),
                    logger=self.logger
                ).run(
                    config=config,
                    dataframe=self.dataframe
                )

                if not dry_run:
                    # Load the mongo module
                    mongo_module = self._get_module("connectors", "mongo_connector")
                    # Execute the mongo load
                    _ = mongo_module.Connector(config=config.get("mongodb"), logger=self.logger).write(dataframe=self.dataframe)
                else:
                    self._dry_run()
                    print(self.dataframe)

            case "load":
                print("test")
            case _:
                self.logger.error("No valid stage type in configuration.")
                raise ValueError("Invalid stage type for main, should be either extract or load")

        return config

    def _dry_run(self):


        match self.dry_run:
            case "d":
                print(self.dataframe)

            case "df":
                self.dataframe.to_json()

    @staticmethod
    def _get_module(module_source: str, module_type: str):
        mod_name = f"{module_source}.{module_type}"
        module = importlib.import_module(mod_name)
        return module

    def _log_start(self, config: dict[str, Any] | None, stage = None):
        if stage:
            self.logger.info(f"Starting Job Stage: {stage}")
            self.logger.debug(f"Config: {config}")
        else:
            self.logger.info(f"Starting job: {self.job_name}")
            self.logger.debug(f"Config: {self.pipeline_config}")

    def _log_end(self, success: bool, records_processed: int = 0, stage = None):
        status = "Success" if success else "Failed"

        if stage:
            self.logger.info(f"Job Stage {stage} complete with status: {status}")
        else:
            self.logger.info(f"Job {self.job_name} complete with status: {status}")
    
        self.logger.info(f"Records processed: {records_processed}")

def run_job(config_file: str, logger, stage: str, dry_run: str):
    config = get_config(config_file, logger)
    Run(config, stage, dry_run).execute() 


def get_config(config_file: str, logger):
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

            return config
    except FileNotFoundError:
        logger.error(f"Unable to open file: {config_file}")
        raise Exception("Missing config for execution")

def setup_logs(loglevel: str, job_name: str, environment: str):

    logname = f"logs/ingest_engine_logs.log"

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)

    handler = logging.FileHandler(logname, mode='a')

    formatter = JsonFormatter(
        "%(asctime)s %(name)s %(levelname)s %(message)s",
        datefmt='%Y-%m-%d %H:%M:%S',
        static_fields={
            "job_name": job_name,
            "environment": environment,
            "execution_id": str(uuid.uuid4())
        }
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if loglevel:
        match loglevel.lower():
            case "notset":
                logger.setLevel(logging.NOTSET)
            case "debug":
                logger.setLevel(logging.DEBUG)
            case "info":
                logger.setLevel(logging.INFO)
            case "warning":
                logger.setLevel(logging.WARNING)
            case "error":
                logger.setLevel(logging.ERROR)
            case "critical":
                logger.setLevel(logging.CRITICAL)
            case _:
                logger.setLevel(logging.INFO)
    else:
        logger.setLevel(logging.INFO)
    
    return logger


def add_args():
    parser = argparse.ArgumentParser(
        description="A simple data ingestion engine",
        epilog="Thanks for using this program!"
    )

    parser.add_argument("-c", "--config", type=str, required=True, help="Path to config file")
    parser.add_argument('-e', "--environment", type=str, required=True, help="Environment the run is targeting")
    parser.add_argument("-s", "--stage", type=str, required=True, help="Config stage name")

    parser.add_argument("-d", "--dryrun", type=str, required=False, help="Dry Run option, d = on screen, df = dry run file")
    parser.add_argument("-l", "--loglevel", type=str, help="Level for the logs, default Info")

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = add_args()
    config = args.config
    stage = args.stage
    dry_run = args.dryrun
    logger = setup_logs(args.loglevel, config.split("/")[-1][:-5], args.environment)
    run_job(config, logger, stage, dry_run)