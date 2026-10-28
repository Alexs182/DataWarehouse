# tests/test_mongo_connector.py
import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from connectors.mongo_connector import Connector  # adjust to your real module path

MODULE = "connectors.mongo_connector"  # used as the patch target prefix


@pytest.fixture
def logger():
    return MagicMock()


@pytest.fixture
def connector(logger):
    with patch(f"{MODULE}.load_dotenv"):
        return Connector(logger)


@pytest.fixture
def config():
    return {
        "connection": "MONGO_URL",
        "database": "test_db",
        "collection_name": "test_collection",
        "execution_type": "write",
    }


@pytest.fixture
def df():
    return pd.DataFrame([{"id": 1, "name": "a"}, {"id": 2, "name": "b"}])


# ---------- connect ----------

def test_connect_success(connector, config, monkeypatch):
    monkeypatch.setenv("MONGO_URL", "mongodb://localhost:27017")

    with patch(f"{MODULE}.pymongo.MongoClient") as mock_client_cls:
        mock_client = mock_client_cls.return_value
        mock_db = mock_client.__getitem__.return_value

        mock_client.list_database_names.return_value = ["test_db"]

        connector.connect(config)

        mock_client_cls.assert_called_once_with("mongodb://localhost:27017")
        mock_client.admin.command.assert_called_once_with("ping")
        mock_client.__getitem__.assert_called_once_with("test_db")
        mock_db.__getitem__.assert_called_once_with("test_collection")
        assert connector.collection is mock_db.__getitem__.return_value


def test_connect_missing_env_var_raises(connector, config, monkeypatch):
    monkeypatch.delenv("MONGO_URL", raising=False)

    with patch(f"{MODULE}.pymongo.MongoClient"):
        with pytest.raises(Exception, match="connection url not found"):
            connector.connect(config)

    connector.logger.error.assert_called_once()


def test_connect_ping_failure_logs_and_reraises(connector, config, monkeypatch):
    monkeypatch.setenv("MONGO_URL", "mongodb://localhost:27017")

    with patch(f"{MODULE}.pymongo.MongoClient") as mock_client_cls:
        mock_client_cls.return_value.admin.command.side_effect = ConnectionError("boom")

        with pytest.raises(ConnectionError):
            connector.connect(config)

    connector.logger.error.assert_called_once_with("Could not establish connection to server.")

def test_check_database_passes_when_present(connector):
    connector.client = MagicMock()
    connector.client.list_database_names.return_value = ["admin", "test_db"]

    connector.check_database("test_db")  # should not raise


def test_check_database_raises_when_missing(connector):
    connector.client = MagicMock()
    connector.client.list_database_names.return_value = ["admin"]

    with pytest.raises(ValueError, match="not found"):
        connector.check_database("test_db")

    connector.logger.error.assert_called_once()


def test_check_server_logs_and_reraises(connector):
    connector.client = MagicMock()
    connector.client.admin.command.side_effect = ConnectionError("boom")

    with pytest.raises(ConnectionError):
        connector.check_server()

    connector.logger.error.assert_called_once_with("Could not establish connection to server.")

# ---------- write ----------

def test_write_empty_dataframe_returns_zero(connector):
    connector.collection = MagicMock()

    result = connector.write(pd.DataFrame())

    assert result == 0
    connector.collection.insert_many.assert_not_called()


def test_write_inserts_records_and_disconnects(connector, df):
    connector.collection = MagicMock()
    connector.client = MagicMock()

    result = connector.write(df)

    assert result == 2
    connector.collection.insert_many.assert_called_once_with(df.to_dict("records"))
    connector.client.close.assert_called_once()


def test_write_insert_failure_logs_and_reraises(connector, df):
    connector.collection = MagicMock()
    connector.collection.insert_many.side_effect = RuntimeError("insert failed")

    with pytest.raises(RuntimeError):
        connector.write(df)

    connector.logger.error.assert_called_once_with("insert failed")


# ---------- run ----------

def test_run_write_dispatches(connector, config, df):
    with patch.object(connector, "connect") as mock_connect, \
         patch.object(connector, "write", return_value=2) as mock_write:
        connector.run(config, df)

    mock_connect.assert_called_once_with(config=config)
    mock_write.assert_called_once_with(dataframe=df)


def test_run_unknown_execution_type_does_not_write(connector, config, df):
    config["execution_type"] = "read"
    with patch.object(connector, "connect"), \
         patch.object(connector, "write") as mock_write:
        connector.run(config, df)

    mock_write.assert_not_called()