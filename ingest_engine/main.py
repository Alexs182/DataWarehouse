
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

class Run:
    def __init__(self, config):
        self.pipeline_config = config
        self.logger = logging.getLogger(__name__)
        self.job_name = self.pipeline_config['job_name']

        self.dataframe = pd.DataFrame()

        self._log_start(self.pipeline_config)

        self.execution_index = 0

    def execute(self):
        self.execution_index += 1

        for stage_index, stage in enumerate(self.pipeline_config['stages']):
            try:
                logger.info(f'Starting Stage: {stage_index} on exexcution index: {self.execution_index}')
                logger.info(f"Stage Config: {stage}") 

                stage['index'] = stage_index
                stage['execution_index'] = self.execution_index

                config = self._execute_stage(
                    pipeline_config=copy.deepcopy(self.pipeline_config),
                    stage_config=stage
                )

                # Dont allow more that 5 executions, great way to ddos a service and get banned
                if self.pipeline_config != config and self.execution_index > 5:
                    self.logger.info("Pipeline Config changed by process, executing subsequent stages.")
                    self.logger.info(f"New Config: {config}")

                    self.pipeline_config = config
                    self.execute()
                    break

            except Exception as e:
                self.logger.error(f"Job failed: {e}", exc_info=True)
                self._log_end(success=False)
        
        self._log_end(success=True)

    def _execute_stage(
            self, 
            pipeline_config,
            stage_config
        ) -> dict[str, Any]:

        connector_module = self._get_module("connectors", stage_config.get('connector_type', ''))
        dataframe, config = connector_module.Connector(
            mapper=stage_config.get("mapper"),
            connection=stage_config.get('connection'),
            logger=self.logger
        ).run(
            pipeline_config=pipeline_config,
            stage_config=stage_config,
            dataframe=self.dataframe
        )

        self.dataframe = dataframe
        return config

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

def run_job(config_file: str, logger):
    config = get_config(config_file, logger)
    Run(config).execute() 


def get_config(config_file: str, logger):
    try:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

            return config
    except FileNotFoundError:
        logger.error(f"Unable to open file: {config_file}")
        raise Exception("Missing config for execution")

def setup_logs(
        loglevel: str, 
        job_name: str, 
        environment: str
    ):

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
    parser.add_argument("-l", "--loglevel", type=str, help="Level for the logs, default Info")
    parser.add_argument('-e', "--environment", type=str, required=True, help="Environment the run is targeting")

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = add_args()
    config = args.config
    logger = setup_logs(args.loglevel, config.split("/")[-1][:-5], args.environment)
    run_job(config, logger)