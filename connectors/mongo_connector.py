
import os
from typing import Dict, Any

import pymongo
import pandas as pd
from dotenv import load_dotenv

from connectors.common import Common

class Connector(Common):
    def __init__(self, logger):
        
        load_dotenv()
        self.logger = logger

    def check_server(self) -> None:
        try:
            self.client.admin.command("ping")
        except Exception:
            self.logger.error("Could not establish connection to server.")
            raise

    def check_database(self, database: str) -> None:
        if database not in self.client.list_database_names():
            self.logger.error(f"Database '{database}' not found on server.")
            raise ValueError(f"Database '{database}' not found.")

    def connect(self, config: Dict[str, Any]) -> None:
        self.server_url = os.getenv(config.get("connection", ""))
        if not self.server_url:
            self.logger.error("Connection url not found in environment variables, check .env file and config.")
            raise Exception("MongoDB connection url not found.")

        self.client = pymongo.MongoClient(self.server_url, serverSelectionTimeoutMS=30000)
        self.check_server()

        database = config.get("database", "")
        self.check_database(database)

        self.db = self.client[database]
        self.collection = self.db[config.get("collection_name", "")]


    def disconnect(self):
        self.client.close()
        self.logger.info("Mongo Connection closed.")

    def write(self, dataframe: pd.DataFrame) -> int:
        recordCount = len(dataframe.index)
        if recordCount < 1:
            self.logger.info("No records to insert!")
            return recordCount

        try:
            self.collection.insert_many(dataframe.to_dict('records'))
        except Exception as e:
            self.logger.error(f"{e}")
            raise

        return recordCount

    def run(self, config: Dict[str, Any], dataframe: pd.DataFrame):

        self.connect(config=config)

        try:
            match config.get('execution_type'):
                case "write":
                    record_count = self.write(dataframe=dataframe)
        finally:
            self.disconnect()
            
        