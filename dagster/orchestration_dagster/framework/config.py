"""Typed config models + loader.

Same YAML schema as the Composer repo, but here we validate it with Pydantic at load time. A config
file that doesn't match the schema fails fast when the code location loads, instead of at DAG parse time.
"""
import os
from pathlib import Path

import yaml
from pydantic import BaseModel, Field

CONFIG_ROOT = Path(__file__).resolve().parent.parent / "configs"


# --- shared --------------------------------------------------------------
class Tag(BaseModel):
    name: str


class DagAttr(BaseModel):
    tags: list[Tag] = Field(default_factory=list)
    alert_after_run_minutes: int | None = None


# --- gcs_to_bigquery -----------------------------------------------------
class GcsSource(BaseModel):
    landing_bucket: str
    file_pattern: str
    field_delimiter: str = ","
    skip_leading_rows: int = 0
    quote_char: str = '"'
    max_bad_records: int = 0
    is_truncate: bool = False


class DataflowOpts(BaseModel):
    max_workers: int = 4
    disk_size_gb: int = 25
    machine_type: str = "n1-standard-2"


class GcsSink(BaseModel):
    raw_dataset: str
    raw_table: str
    error_bucket: str


class GcsToBqExec(BaseModel):
    source: GcsSource
    dataflow: DataflowOpts = Field(default_factory=DataflowOpts)
    sink: GcsSink


# --- dataform ------------------------------------------------------------
class DataformName(BaseModel):
    name: str | None = None
    database: str | None = None
    schema_: str | None = Field(default=None, alias="schema")
    model_config = {"populate_by_name": True}


class DataformExec(BaseModel):
    include_dependencies: bool = False
    include_dependents: bool = False
    do_full_refresh: bool = False
    names: list[DataformName]


class Schedule(BaseModel):
    interval: str | None = None
    timezone: str = "UTC"
    executions: dict = Field(default_factory=dict)


class PipelineConfig(BaseModel):
    """One YAML file = one pipeline. `pattern` and `name` are derived from the file path."""
    pattern: str
    name: str
    dag_attr: DagAttr = Field(default_factory=DagAttr)
    workflow_type: str = "dataflow"
    schedules: list[Schedule] = Field(default_factory=list)

    @property
    def pipeline_id(self) -> str:
        return f"{self.pattern}__{self.name}"

    @property
    def tag_names(self) -> list[str]:
        return [t.name for t in self.dag_attr.tags]

    @property
    def cron(self) -> str | None:
        for s in self.schedules:
            if s.interval:
                return s.interval
        return None


def load_configs(pattern: str) -> list[PipelineConfig]:
    """Load + validate every config under configs/<pattern>/."""
    out: list[PipelineConfig] = []
    pattern_dir = CONFIG_ROOT / pattern
    if not pattern_dir.is_dir():
        return out
    for path in sorted(pattern_dir.glob("*.y*ml")):
        raw = yaml.safe_load(path.read_text()) or {}
        raw["pattern"] = pattern
        raw["name"] = path.stem
        out.append(PipelineConfig(**raw))
    return out


def env(key: str, default: str = "") -> str:
    return os.getenv(key, default)
