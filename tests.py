import sys
import logging
import unittest
from unittest.mock import MagicMock, patch, mock_open
import pandas as pd

# The module under test is assumed to be named `run` (run.py)
import importlib, types

# ── inline stub so we can import without the real connectors package ──────────
connectors_stub = types.ModuleType("connectors")
sys.modules.setdefault("connectors", connectors_stub)

import main as run_module
from main import Run, get_config, setup_logs, add_args


# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_config(overrides=None):
    base = {
        "job_name": "test_job",
        "extract": {
            "connector_type": "csv_connector",
            "mapper": {"col_a": "a"},
        },
        "target": {
            "connector_type": "db_connector",
            "mapper": {"col_a": "a"},
            "connection": {"host": "localhost"},
        },
    }
    if overrides:
        base.update(overrides)
    return base


def _make_runner(config=None):
    return Run(config or _make_config())


# ─────────────────────────────────────────────────────────────────────────────
# Run.__init__
# ─────────────────────────────────────────────────────────────────────────────

class TestRunInit(unittest.TestCase):
    def test_job_name_set_from_config(self):
        runner = _make_runner()
        self.assertEqual(runner.job_name, "test_job")

    def test_config_stored(self):
        cfg = _make_config()
        runner = Run(cfg)
        self.assertIs(runner.config, cfg)

    def test_logger_created(self):
        runner = _make_runner()
        self.assertIsInstance(runner.logger, logging.Logger)


# ─────────────────────────────────────────────────────────────────────────────
# Run.execute
# ─────────────────────────────────────────────────────────────────────────────

class TestRunExecute(unittest.TestCase):
    def _runner_with_extract(self, return_value):
        runner = _make_runner()
        runner._extract = MagicMock(return_value=return_value)
        return runner

    def test_execute_calls_extract(self):
        runner = self._runner_with_extract(5)
        runner.execute()
        runner._extract.assert_called_once_with(config=runner.config["extract"])

    def test_execute_exits_on_exception(self):
        runner = _make_runner()
        runner._extract = MagicMock(side_effect=RuntimeError("boom"))
        with self.assertRaises(SystemExit) as ctx:
            runner.execute()
        self.assertEqual(ctx.exception.code, 1)


# ─────────────────────────────────────────────────────────────────────────────
# Run._extract
# ─────────────────────────────────────────────────────────────────────────────

class TestRunExtract(unittest.TestCase):
    def _setup(self, rows):
        runner = _make_runner()

        fake_df = pd.DataFrame({"col_a": range(rows)})
        mock_connector_instance = MagicMock()
        mock_connector_instance.run.return_value = fake_df

        mock_connector_class = MagicMock(return_value=mock_connector_instance)
        mock_module = MagicMock()
        mock_module.Connector = mock_connector_class

        runner._get_module = MagicMock(return_value=mock_module)
        runner._write_to_datastore = MagicMock()
        return runner, fake_df

    def test_returns_record_count(self):
        runner, _ = self._setup(7)
        count = runner._extract(config=runner.config["extract"])
        self.assertEqual(count, 7)

    def test_calls_write_when_rows_present(self):
        runner, _ = self._setup(3)
        runner._extract(config=runner.config["extract"])
        runner._write_to_datastore.assert_called_once()

    def test_skips_write_when_empty(self):
        runner, _ = self._setup(0)
        runner._extract(config=runner.config["extract"])
        runner._write_to_datastore.assert_not_called()

    def test_get_module_called_with_connector_type(self):
        runner, _ = self._setup(1)
        runner._extract(config=runner.config["extract"])
        runner._get_module.assert_called_once_with("connectors", "csv_connector")


# ─────────────────────────────────────────────────────────────────────────────
# Run._write_to_datastore
# ─────────────────────────────────────────────────────────────────────────────

class TestWriteToDatastore(unittest.TestCase):
    def test_calls_target_connector_run(self):
        runner = _make_runner()
        fake_df = pd.DataFrame({"col_a": [1, 2]})

        mock_connector_instance = MagicMock()
        mock_module = MagicMock()
        mock_module.Connector.return_value = mock_connector_instance
        runner._get_module = MagicMock(return_value=mock_module)

        runner._write_to_datastore(dataframe=fake_df, config=runner.config)

        mock_connector_instance.run.assert_called_once_with(
            dataframe=fake_df, config=runner.config["target"]
        )

    def test_uses_target_connector_type(self):
        runner = _make_runner()
        fake_df = pd.DataFrame({"col_a": [1]})

        mock_module = MagicMock()
        runner._get_module = MagicMock(return_value=mock_module)

        runner._write_to_datastore(dataframe=fake_df, config=runner.config)

        runner._get_module.assert_called_once_with("connectors", "db_connector")


# ─────────────────────────────────────────────────────────────────────────────
# Run._get_module
# ─────────────────────────────────────────────────────────────────────────────

class TestGetModule(unittest.TestCase):
    def test_imports_correct_module(self):
        fake_mod = types.ModuleType("connectors.fake")
        with patch("importlib.import_module", return_value=fake_mod) as mock_import:
            result = Run._get_module("connectors", "fake")
            mock_import.assert_called_once_with("connectors.fake")
            self.assertIs(result, fake_mod)

    def test_raises_on_missing_module(self):
        with patch("importlib.import_module", side_effect=ModuleNotFoundError):
            with self.assertRaises(ModuleNotFoundError):
                Run._get_module("connectors", "nonexistent")


# ─────────────────────────────────────────────────────────────────────────────
# get_config
# ─────────────────────────────────────────────────────────────────────────────

class TestGetConfig(unittest.TestCase):
    def _logger(self):
        return logging.getLogger("test")

    def test_parses_valid_yaml(self):
        yaml_content = "job_name: my_job\nextract:\n  connector_type: csv\n"
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            cfg = get_config("fake.yaml", self._logger())
        self.assertEqual(cfg["job_name"], "my_job")

    def test_raises_on_missing_file(self):
        with patch("builtins.open", side_effect=FileNotFoundError):
            with self.assertRaises(Exception, msg="Missing config for execution"):
                get_config("missing.yaml", self._logger())

    def test_returns_dict(self):
        yaml_content = "job_name: x\n"
        with patch("builtins.open", mock_open(read_data=yaml_content)):
            cfg = get_config("fake.yaml", self._logger())
        self.assertIsInstance(cfg, dict)


# ─────────────────────────────────────────────────────────────────────────────
# setup_logs
# ─────────────────────────────────────────────────────────────────────────────

class TestSetupLogs(unittest.TestCase):
    def _call(self, level):
        with patch("logging.basicConfig"):
            return setup_logs(level, "test_job")

    def test_returns_logger(self):
        logger = self._call("info")
        self.assertIsInstance(logger, logging.Logger)

    @patch("logging.basicConfig")
    def test_debug_level(self, _):
        logger = setup_logs("debug", "test_job")
        self.assertEqual(logger.level, logging.DEBUG)

    @patch("logging.basicConfig")
    def test_warning_level(self, _):
        logger = setup_logs("warning", "test_job")
        self.assertEqual(logger.level, logging.WARNING)

    @patch("logging.basicConfig")
    def test_unknown_level_defaults_to_info(self, _):
        logger = setup_logs("verbose", "test_job")
        self.assertEqual(logger.level, logging.INFO)

    @patch("logging.basicConfig")
    def test_none_level_defaults_to_info(self, _):
        logger = setup_logs(None, "test_job")
        self.assertEqual(logger.level, logging.INFO)

    @patch("logging.basicConfig")
    def test_case_insensitive(self, _):
        logger = setup_logs("ERROR", "test_job")
        self.assertEqual(logger.level, logging.ERROR)


# ─────────────────────────────────────────────────────────────────────────────
# add_args
# ─────────────────────────────────────────────────────────────────────────────

class TestAddArgs(unittest.TestCase):
    def test_config_required(self):
        with patch("sys.argv", ["prog"]):
            with self.assertRaises(SystemExit):
                add_args()

    def test_config_parsed(self):
        with patch("sys.argv", ["prog", "-c", "path/to/config.yaml"]):
            args = add_args()
        self.assertEqual(args.config, "path/to/config.yaml")

    def test_loglevel_optional(self):
        with patch("sys.argv", ["prog", "-c", "cfg.yaml"]):
            args = add_args()
        self.assertIsNone(args.loglevel)

    def test_loglevel_parsed(self):
        with patch("sys.argv", ["prog", "-c", "cfg.yaml", "-l", "debug"]):
            args = add_args()
        self.assertEqual(args.loglevel, "debug")


if __name__ == "__main__":
    unittest.main()