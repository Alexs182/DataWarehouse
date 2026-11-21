from abc import ABC, abstractmethod
from typing import Any, Dict

import pandas as pd

from connectors.common import Common


class BaseConnector(Common, ABC):
    """Contract every connector must satisfy. Instantiating a subclass that
    doesn't implement run() fails immediately with a TypeError."""

    def __init__(self, logger):
        self.logger = logger
        self.config: Dict[str, Any] = {}

    @abstractmethod
    def run(self, config: Dict[str, Any], dataframe: pd.DataFrame) -> pd.DataFrame:
        """Execute the stage described by `config`; return the resulting dataframe."""