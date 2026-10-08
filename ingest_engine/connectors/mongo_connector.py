
import os

import pymongo
import pandas as pd
from dotenv import load_dotenv

from connectors.common import Common

class Connector(Common):
    def __init__(self, config, logger):
        
        load_dotenv()
        self.logger = logger
        self.config = config

        self.server_url = os.getenv(self.config.get("connection", ""))

        if self.server_url == "None":
            self.logger.error(f"Connection: {self.server_url} not found in environment variables, check .env file and config.")
            raise Exception("MongoDB connection url not found.")

        self.connect()

    def connect(self):
        try:
            self.client = pymongo.MongoClient(self.server_url)
            self.db = self.client[self.config.get("database", "")]
            self.collection = self.db[self.config.get("collection_name", "")]
        except:
            self.logger.info('Could not establish connection to server.')

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
