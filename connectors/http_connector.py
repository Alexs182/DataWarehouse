from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import HTTPError, RequestException
from urllib3.util.retry import Retry

from connectors.base import BaseConnector

SECRET_SENTINEL = "ENVsecret"
SUPPORTED_METHODS = ("GET",)  # extend deliberately, retries below are GET-only
DEFAULT_TIMEOUT: Tuple[float, float] = (5, 30)  # (connect, read) seconds


def make_session(total_retries: int = 3, backoff: float = 1.0) -> requests.Session:
    """Session that retries transient failures and honours Retry-After on 429s."""
    retry = Retry(
        total=total_retries,
        backoff_factor=backoff,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=SUPPORTED_METHODS,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session = requests.Session()
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


class HttpConnector(BaseConnector):
    def __init__(self, logger):
        super().__init__(logger)
        self.raw_data: Any = []
        self._session: Optional[requests.Session] = None

    def get_raw_response_data(self) -> Any:
        return self.raw_data

    # ------------------------------------------------------------------ #
    # Secrets
    # ------------------------------------------------------------------ #
    def _resolve_secrets(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """Return a copy of params with sentinel values swapped for real secrets.

        The input dict (which lives inside self.config) is never mutated.
        """
        resolved = dict(params)
        secret_map = self.config.get("secrets", {})

        for key, value in params.items():
            if value != SECRET_SENTINEL:
                continue
            env_name = secret_map.get(key)
            if not env_name:
                raise ValueError(
                    f"Param '{key}' is marked {SECRET_SENTINEL} but has no entry "
                    f"under config['secrets']."
                )
            resolved[key] = self.require_env(env_name)

        # TODO: same treatment for headers when an endpoint needs it.
        return resolved

    @staticmethod
    def _redact(text: str, secrets: List[str]) -> str:
        for secret in secrets:
            if secret:
                text = text.replace(str(secret), "***")
        return text

    # ------------------------------------------------------------------ #
    # HTTP
    # ------------------------------------------------------------------ #
    def _get_session(self) -> requests.Session:
        if self._session is None:
            self._session = make_session()
        return self._session

    def fetch(self, endpoint: str, params: Dict[str, Any], method: str = "GET") -> Any:
        method = (method or "GET").upper()
        if method not in SUPPORTED_METHODS:
            raise ValueError(
                f"Unsupported HTTP method '{method}'. Supported: {SUPPORTED_METHODS}"
            )

        request_params = self._resolve_secrets(params)
        secret_values = [
            str(request_params[k]) for k, v in params.items() if v == SECRET_SENTINEL
        ]
        timeout = tuple(self.config.get("timeout", DEFAULT_TIMEOUT))

        try:
            response = self._get_session().request(
                method, endpoint, params=request_params, timeout=timeout
            )
            response.raise_for_status()
        except HTTPError as e:
            status = e.response.status_code if e.response is not None else "unknown"
            reason = e.response.reason if e.response is not None else ""
            # `from None` drops the chained exception, whose text contains the full URL.
            raise HTTPError(
                f"HTTP {status} {reason} from {endpoint}"
            ) from None
        except RequestException as e:
            # Connection errors / timeouts can embed the query string too.
            msg = self._redact(str(e), secret_values)
            raise RequestException(
                f"Request to {endpoint} failed: {type(e).__name__}: {msg}"
            ) from None

        try:
            data = response.json()
        except ValueError:
            raise ValueError(
                f"Response from {endpoint} was not valid JSON "
                f"(status {response.status_code}, {len(response.content)} bytes)."
            ) from None

        count = len(data) if isinstance(data, list) else 1
        self.logger.info(f"Raw response record count: {count}.")
        return data

    # ------------------------------------------------------------------ #
    # Pipeline
    # ------------------------------------------------------------------ #
    def _ingest(self, endpoint: str, method: str, params: Dict[str, Any]) -> Any:
        self.raw_data = self.fetch(endpoint=endpoint, params=params, method=method)

        # entry point for raw schema derivation
        if self.config.get("data_schema", {}).get("active"):
            self.get_schema(
                schema_type="raw_schema",
                logger=self.logger,
                records=self.raw_data,
                pipeline_name=self.config.get("job_name", ""),
            )

        return self.raw_data

    def run(self, config: Dict[str, Any], dataframe: pd.DataFrame) -> pd.DataFrame:
        self.config = config

        try:
            data = self._ingest(
                endpoint=config.get("endpoint", ""),
                method=config.get("method", "GET"),
                params=config.get("params", {}),
            )
        finally:
            if self._session is not None:
                self._session.close()
                self._session = None

        return self.to_dataframe(
            records=data, shape_config=config.get("source_shape")
        )