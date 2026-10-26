
import os
import sys
import yaml
import copy
import uuid
import argparse
import logging
import importlib
from typing import Any, Dict

#from pythonjsonlogger.json import JsonFormatter
import pandas as pd

from connectors.common import Common

class Run(Common):
    def __init__(self, config, args: Dict[str, Any]):
        self.dry_run = args['dry_run']
        self.dry_run_file_path = args['dry_run_file_path']
        self.stage = args['stage']

        self.pipeline_config = config
        self.job_name = self.pipeline_config['job_name']

        self.dataframe = pd.DataFrame()

        self.logger = logging.getLogger(__name__)
        self._log_start(self.pipeline_config)


    def execute(self):
        stages = self.pipeline_config.get('stages')
        if not stages:
            self.logger.error(f"Invalid config file missing 'stages' block.")
            raise

        # Overide to execute single stage from pipeline
        if self.stage:
            stages = [s for s in stages if s.get('stage_name') == self.stage]
            if not stages:
                self.logger.error(f"Override stage_name: {self.stage} not found in config.")
                raise

        stage_names = [s.get('stage_name', 'UNNAMED') for s in stages]
        self.logger.info(f"Execution identified {len(stage_names)} to run: {', '.join(stage_names)}")

        try:
            for stage in stages:

                stage_name = stage.get('stage_name')
                self.logger.info(f"Starting Execution of stage: {stage.get('stage_name', 'UNNAMED')}")

                # Check if the pipeline stage is enabled, primarily for development
                if stage.get("is_active") is False:
                    self.logger.info(f"Stage {stage_name} is inactive.")
                    break  

                match stage.get('stage_type'):
                    case "connector":

                        # bypass write operations in dry run
                        if stage.get('operation_type') == "write" and self.dry_run:
                            self.logger.info(f"Stage {stage_name} bypassing write operation due to dry run mode active")
                            break

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
                        self.logger.error(f"Invalid stage_type in stage: {stage_name}")
                        raise


                # If the pipeline execution is in debug mode each stage's output
                # is printed to screen
                if self.dry_run:
                    self._dry_run(stage_name)
                

        except Exception as e:
            self.logger.error(f"Job failed: {e}", exc_info=True)
            self._log_end(success=False)
            raise
        
        self._log_end(success=True)

    def _dry_run(self, stage_name: str):
        print(self.dataframe)

        if self.dry_run_file_path:
            self.dataframe.to_json(os.path.join(self.dry_run_file_path, f"{stage_name}.json"))

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

def run_job(args: Dict[str, Any], logger):
    config = get_config(args['config'], logger)
    Run(config, args).execute() 


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

    parser.add_argument("-d", "--dryrun", type=str, required=False, help="Optional dry run mode")
    parser.add_argument("-df", "--dryrunpath", type=str, required=False, help="Dry Run filepath, optional to store dry run outputs in file")
    parser.add_argument("-l", "--loglevel", type=str, help="Level for the logs, default Info")

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = add_args()

    args = {
        "config": args.config,
        "stage": args.stage,
        "environment": args.environment,
        "dry_run": args.dryrun,
        "dry_run_file_path": args.dryrunpath,
        "log_level": args.loglevel
    }
    logger = setup_logs(args['log_level'], args['config'].split("/")[-1][:-5], args['environment'])

    try:
        run_job(args, logger)
    except Exception as e:
        logger.error(e)
        sys.exit(1)