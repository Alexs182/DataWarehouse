from typing import Dict, Type

from connectors.base import BaseConnector
from connectors.http_connector import HttpConnector
from connectors.mongo_connector import MongoConnector
from connectors.postgres_connector import PostgresConnector
from connectors.file_connector import FileConnector
from errors import ConfigError

# Keys are the `connector_type` values used in the YAML configs.
CONNECTORS: Dict[str, Type[BaseConnector]] = {
    "http": HttpConnector,
    "mongo": MongoConnector,
    "postgres": PostgresConnector,
    "file": FileConnector
}


def get_connector(connector_type: str) -> Type[BaseConnector]:
    try:
        return CONNECTORS[connector_type]
    except KeyError:
        raise ConfigError(
            f"Unknown connector_type {connector_type!r}. "
            f"Expected one of: {', '.join(CONNECTORS)}"
        ) from None