

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

            self._log_end(success=True, record_processed=records)

        except Exception as e:
            self.logger.error(f"Job failed: {e}", exc_info=True)
            self._log_end(success=False)

    def _extract(self, config: dict[str, any]):

        connector_module = self._get_module("connectors", config.get('connector_type'))

        dataframe = connector_module.Connector(config.get("mapper")).run(config=config)

        self._write_to_datastore(dataframe=dataframe, config=self.config)

        records_processed = dataframe.count()

        return records_processed
    
    def _write_to_datastore(
            self,
            dataframe,
            config: dict[str, any]
        ):

        self._log_start(stage=f"DataStore write", config=config)

        target = config.get('target')
        target_connector_module = self._get_module("connectors", target.get('connector_type'))

        target_connector_module.Connector(mapper=target.get('mapper')).run(dataframe=dataframe, config=target)

        self._log_end(stage=f"DataStore write", success=True, records_processed=len(dataframe))

    @staticmethod
    def _get_module(module_source: str, module_type: str):
        mod_name = f"{module_source}.{module_type}"
        module = importlib.import_module(mod_name)
        return module

    def _log_start(self, config: dict[str, any] | None, stage = None):
        if stage:
            self.logger.info(f"Starting Job Stage: {stage}")
            self.logger.info(f"Config: {config}")
        else:
            self.logger.info(f"Starting job: {self.job_name}")
            self.logger.info(f"Config: {self.config}")

    def _log_end(self, success: bool, record_processed: int = 0, stage = None):
        status = "Success" if success else "Failed"

        if stage:
            self.logger.info(f"Job Stage {stage} completed: {status}")
        else:
            self.logger.info(f"Job {self.job_name} comple: {status}")
    
        self.logger.info(f"Records processed: {record_processed}")

def run_job(config_file: str):
    config = get_config(config_file)

    logging.basicConfig(level=logging.INFO)

    Run(config).execute() 


def get_config(config_file: str):
    if config_file:
        with open(config_file, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

            return config
    else:
        raise ValueError("Missing config for execution")

def add_args():
    parser = argparse.ArgumentParser(
        description="A simple data ingestion engine",
        epilog="Thanks for using this program!"
    )

    parser.add_argument("-c", "--config", type=str, help="Path to config file")

    args = parser.parse_args()

    return args


if __name__ == "__main__":
    args = add_args()
    run_job(args.config)