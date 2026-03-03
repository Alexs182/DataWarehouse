

import yaml
import argparse
import logging
import importlib

class Run:
    def __init__(self, config):
        self.config = config
        self.logger = logging.getLogger(__name__)
        self.job_name = self.config['job_name']

    def execute(self):
        self._log_start(self.config)

        try:
            records = self._extract(config = self.config["extract"])

            self._log_end(success=True, records_processed=records)

        except Exception as e:
            self.logger.error(f"Job failed: {e}", exc_info=True)
            self._log_end(success=False)

    def _extract(self, config: dict[str, any]):
        connector_module = self._get_module("connectors", config.get('connector_type'))
        dataframe = connector_module.Connector(config.get("mapper")).run(config=config)

        self._write_to_datastore(dataframe=dataframe, config=self.config)

        return len(dataframe)
    
    def _write_to_datastore(
            self,
            dataframe,
            config: dict[str, any]
        ):

        self._log_start(stage=f"DataStore write", config=config)

        target = config.get('target')
        target_connector_module = self._get_module("connectors", target.get('connector_type'))

        target_connector_module.Connector(
            mapper=target.get('mapper'), 
            connection=target.get("connection")
        ).run(dataframe=dataframe, config=target)

        self._log_end(stage=f"DataStore write", success=True, records_processed=len(dataframe))

    @staticmethod
    def _get_module(module_source: str, module_type: str):
        mod_name = f"{module_source}.{module_type}"
        module = importlib.import_module(mod_name)
        return module

    def _log_start(self, config: dict[str, any] | None, stage = None):
        if stage:
            self.logger.info(f"Starting Job Stage: {stage}")
            self.logger.debug(f"Config: {config}")
        else:
            self.logger.info(f"Starting job: {self.job_name}")
            self.logger.debug(f"Config: {self.config}")

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

def setup_logs(loglevel: str, job_name: str):

    logname = f"logs/{job_name}_logs.log"
    logging.basicConfig(
        filename=logname,
        filemode='a',
        format='%(asctime)s,%(msecs)03d %(name)s %(levelname)s %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger = logging.getLogger(__name__)

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

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = add_args()
    config = args.config
    logger = setup_logs(args.loglevel, config.split("/")[-1][:-5])
    run_job(config, logger)