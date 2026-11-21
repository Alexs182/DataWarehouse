"""Typed pipeline config: validated once, up front, with per-stage error messages.
 
Requires pydantic >= 2.5.
"""
from typing import Annotated, Any, Dict, List, Literal, Optional, Tuple, Union
 
from pydantic import (
    BaseModel,
    ConfigDict,
    Discriminator,
    Field,
    Tag,
    ValidationError,
    model_validator,
)
 
SECRET_SENTINEL = "ENVsecret"
 
# Single source of truth for loadable connectors. Add an entry here AND a
# stage model + Tag below when you add a connector module.
CONNECTOR_TYPES = ("http", "mongo", "postgres", "file")
STAGE_TYPES = ("connector", "transform")
 
 
class ConfigError(ValueError):
    pass
 
 
class _Strict(BaseModel):
    # extra="forbid": typos ("enpoint") fail loudly instead of silently defaulting
    model_config = ConfigDict(extra="forbid", frozen=True)
 
 
class DataSchemaConfig(_Strict):
    active: bool = False
 
 
class _StageBase(_Strict):
    stage_name: str = Field(min_length=1)
    is_active: bool = True

class HttpConnectorStage(_StageBase):
    stage_type: Literal["connector"]
    connector_type: Literal["http"]
    operation_type: Literal["read"] = "read"  # the http connector only reads
    endpoint: str = Field(pattern=r"^https?://")
    method: Literal["GET"] = "GET"
    params: Dict[str, Any] = {}
    secrets: Dict[str, str] = {}  # param name -> env var name
    timeout: Tuple[float, float] = (5, 30)
    source_shape: Optional[Dict[str, Any]] = None
    source_name: str = Field(min_lenth=1)
    data_schema: DataSchemaConfig = DataSchemaConfig()
 
    @model_validator(mode="after")
    def _secrets_are_mapped(self):
        missing = [
            k for k, v in self.params.items()
            if v == SECRET_SENTINEL and k not in self.secrets
        ]
        if missing:
            raise ValueError(
                f"params {missing} are marked {SECRET_SENTINEL} "
                f"but have no entry under 'secrets'"
            )
        return self

class MongoConnectorStage(_StageBase):
    stage_type: Literal["connector"]
    connector_type: Literal["mongo"]
    operation_type: Literal["read", "write"]
    execution_type: Literal["read", "write"]
    connection: str = Field(min_length=1)

    database: str = Field(min_length=1)
    collection_name: str = Field(min_length=1)
    

    data_schema: DataSchemaConfig = DataSchemaConfig()


class PostgresConnectorStage(_StageBase):
    stage_type: Literal["connector"]
    connector_type: Literal["postgres"]

class FileConnectorStage(_StageBase):
    stage_type: Literal["connector"]
    connector_type: Literal["file"]
 
class TransformStage(_StageBase):
    stage_type: Literal["transform"]
    mapper: Optional[Union[str, Dict[str, Any]]] = None
    # add the keys your transform stage actually requires here
 
 
def _stage_tag(v: Any) -> Optional[str]:
    get = v.get if isinstance(v, dict) else lambda k: getattr(v, k, None)
    if get("stage_type") == "transform":
        return "transform"
    if get("stage_type") == "connector" and get("connector_type") in CONNECTOR_TYPES:
        return f"connector:{get('connector_type')}"
    return None
 
 
Stage = Annotated[
    Union[
        Annotated[TransformStage, Tag("transform")],
        Annotated[HttpConnectorStage, Tag("connector:http")],
        Annotated[MongoConnectorStage, Tag("connector:mongo")],
        Annotated[PostgresConnectorStage, Tag("connector:postgres")],
        Annotated[FileConnectorStage, Tag("connector:file")],
    ],
    Discriminator(_stage_tag),
]
 
 
class PipelineConfig(_Strict):
    job_name: str = Field(min_length=1)
    stages: List[Stage] = Field(min_length=1)
 
 
def _precheck(raw: Any) -> None:
    """Human-friendly errors for the mistakes that cause the cryptic failures."""
    if not isinstance(raw, dict):
        raise ConfigError("Config file is empty or is not a YAML mapping.")
    job = raw.get("job_name", "<unnamed job>")
    stages = raw.get("stages")
    if not isinstance(stages, list) or not stages:
        raise ConfigError(f"[{job}] 'stages' must be a non-empty list.")
 
    for i, s in enumerate(stages):
        name = s.get("stage_name", f"#{i}") if isinstance(s, dict) else f"#{i}"
        if not isinstance(s, dict):
            raise ConfigError(f"[{job}] stage {name} must be a mapping.")
        if s.get("stage_type") not in STAGE_TYPES:
            raise ConfigError(
                f"[{job}] stage '{name}': stage_type is "
                f"{s.get('stage_type')!r}, expected one of {', '.join(STAGE_TYPES)}."
            )
        if s["stage_type"] == "connector" and s.get("connector_type") not in CONNECTOR_TYPES:
            raise ConfigError(
                f"[{job}] stage '{name}': connector_type is "
                f"{s.get('connector_type')!r}, expected one of {', '.join(CONNECTOR_TYPES)}."
            )
 
 
def load_pipeline_config(raw: Any) -> PipelineConfig:
    _precheck(raw)
    try:
        return PipelineConfig.model_validate(raw)
    except ValidationError as e:
        lines = []
        for err in e.errors():
            loc = [str(p) for p in err["loc"] if ":" not in str(p) and p != "transform"]
            lines.append(f"  - {'.'.join(loc) or '<root>'}: {err['msg']}")
        raise ConfigError(
            f"[{raw.get('job_name', '<unnamed job>')}] invalid config:\n" + "\n".join(lines)
        ) from None
 