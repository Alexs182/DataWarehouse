
import sys
import yaml
import copy
import uuid
import argparse
import logging
import importlib
from typing import Any

#from pythonjsonlogger.json import JsonFormatter
import pandas as pd

from connectors.common import Common

class Run(Common):
    def __init__(self, config, stage_type: str, dry_run: str):
        self.pipeline_config = config
        self.dry_run = dry_run
        self.logger = logging.getLogger(__name__)
        self.job_name = self.pipeline_config['job_name']
        self.stage_type = stage_type

        self.dataframe = pd.DataFrame()

        self._log_start(self.pipeline_config)


    def execute(self):
        stages = self.pipeline_config.get('stages')
        if not stages:
            self.logger.error(f"Invalid config file missing 'stages' block.")
            raise

        # Overide to execute single stage from pipeline
        if self.stage_type:
            stages = [s for s in stages if s.get('stage_name') == self.stage_type]
            if not stages:
                self.logger.error(f"Override stage_name: {self.stage_type} not found in config.")
                raise

        stage_names = [s.get('stage_name', 'UNNAMED') for s in stages]
        self.logger.info(f"Execution identified {len(stage_names)} to run: {', '.join(stage_names)}")

        try:
            for stage in stages:
                self.logger.info(f"Starting Execution of stage: {stage.get('stage_name', 'UNNAMED')}")
                match stage.get('stage_type'):
                    case "connector":
                        connector_module = self._get_module("connectors", stage.get('connector_type', ''))
                        self.dataframe = connector_module.Connector(logger=self.logger).run(
                            config=stage,
                            dataframe=self.dataframe
                        )
                    case "transform":
                        mapper = stage.get("mapper")
                        mapper = self.get_mapper(mapper, self.logger) if mapper else None

                        self.dataframe = self.map_data(logger=self.logger, records=self.dataframe)

                    case _:
                        self.logger.error(f"Invalid stage_type in stage: {stage.get('stage_name')}")


        except Exception as e:
            self.logger.error(f"Job failed: {e}", exc_info=True)
            self._log_end(success=False)
            raise
        
        self._log_end(success=True)

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

    #handler = logging.FileHandler(logname, mode='a')

    #formatter = JsonFormatter(
    #    "%(asctime)s %(name)s %(levelname)s %(message)s",
    #    datefmt='%Y-%m-%d %H:%M:%S',
    #    static_fields={
    #        "job_name": job_name,
    #        "environment": environment,
    #        "execution_id": str(uuid.uuid4())
    #    }
    #)

    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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
    parser.add_argument("-s", "--stage", type=str, help="Config stage name")

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

    try:
        run_job(config, logger, stage, dry_run)
    except Exception as e:
        logger.error(e)
        sys.exit(1)