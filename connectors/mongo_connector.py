
import os

import pymongo
import pandas as pd
from dotenv import load_dotenv

from connectors.common import Common

class Connector(Common):
    def __init__(self, logger):
        
        load_dotenv()
        self.logger = logger

    def connect(self, config: Dict[str, Any]):
        self.server_url = os.getenv(config.get("connection", ""))

        if self.server_url == "None":
            self.logger.error(f"Connection: {self.server_url} not found in environment variables, check .env file and config.")
            raise Exception("MongoDB connection url not found.")

        try:
            self.client = pymongo.MongoClient(self.server_url)
            self.db = self.client[config.get("database", "")]
            self.collection = self.db[config.get("collection_name", "")]
        except:
            self.logger.error('Could not establish connection to server.')
            raise

    def write(self, dataframe: pd.DataFrame) -> int:
        recordCount = len(dataframe.index)
        if recordCount < 1:
            self.logger.info("No records to insert!")
            return recordCount

        try:
            self.collection.insert_many(dataframe.to_dict('records'))
        except Exception as e:
            self.logger.error(f"{e}")

        return recordCount

    def run(self, config: Dict[str, Any], dataframe: pd.DataFrame):

        self.connect(config=config)

        match config.get('execution_type'):
            case "write":
                record_count = self.write(dataframe=dataframe)