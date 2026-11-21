import logging

import pandas as pd
import pytest

from connectors import CONNECTORS, get_connector
from connectors.base import BaseConnector
from errors import ConfigError
from job_config import CONNECTOR_STAGE_MODELS


def test_registry_entries_are_connectors():
    for name, cls in CONNECTORS.items():
        assert issubclass(cls, BaseConnector), name


def test_every_connector_has_a_config_model():
    assert set(CONNECTORS) == set(CONNECTOR_STAGE_MODELS)


@pytest.mark.parametrize("bad", ["", "transforms", "HTTP"])
def test_unknown_type_gives_clear_error(bad):
    with pytest.raises(ConfigError, match="Expected one of: http"):
        get_connector(bad)


def test_incomplete_connector_cannot_be_instantiated():
    class Broken(BaseConnector):
        pass

    with pytest.raises(TypeError):
        Broken(logging.getLogger())


def test_fake_connector_via_registry(monkeypatch):
    """The payoff: swap in a fake without touching imports or the network."""
    class Fake(BaseConnector):
        def run(self, config, dataframe):
            return pd.DataFrame({"x": [1, 2, 3]})

    monkeypatch.setitem(CONNECTORS, "fake", Fake)
    out = get_connector("fake")(logging.getLogger()).run({}, pd.DataFrame())
    assert len(out) == 3