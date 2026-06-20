"""Build-time DAG generator.

Reads every YAML config under dags/configs/<pattern>/ and renders the matching Jinja template in
framework/templates/<pattern>.py into a concrete dags/<pattern>__<name>.py file. CI runs this, then
rsyncs dags/ + configs to the Composer bucket.

This is the "framework" entrypoint: a new pipeline is a new YAML file, never new DAG code.

Usage:
    python framework/dag_builder.py          # generate all
    python framework/dag_builder.py dataform # generate one pattern
"""
import os
import sys

from framework.utils import jinja_utils
from dags.utils import common_utils

DAG_DIR = "dags"
CONFIG_ROOT = f"{DAG_DIR}/configs"
TEMPLATE_DIR = "framework/templates"

# Patterns supported by the framework. Add a template + a config folder to extend.
PATTERNS = ["gcs_to_bigquery", "dataform"]


def build(pattern: str) -> None:
    config_dir = f"{CONFIG_ROOT}/{pattern}"
    template_path = f"{TEMPLATE_DIR}/{pattern}.py"
    if not os.path.isdir(config_dir):
        print(f"[skip] no config dir for pattern '{pattern}'")
        return

    with open(template_path, encoding="utf-8") as f:
        template = f.read()

    for filename in sorted(os.listdir(config_dir)):
        path = os.path.join(config_dir, filename)
        if not os.path.isfile(path) or not filename.endswith((".yaml", ".yml")):
            continue

        name = os.path.splitext(filename)[0]
        dag_id = f"{pattern}__{name}"
        config = common_utils.get_yaml_file_contents(path)

        # Derive DAG-level attributes from the config (shared logic across patterns).
        schedule = common_utils.generate_schedule(config)
        tags = common_utils.parse_tags(config)
        alert_after = str(common_utils.parse_alert_after_run_minutes(config))
        outlets = common_utils.parse_outlets(config)

        # Write the raw template then render its Jinja against these env values.
        out_path = os.path.join(DAG_DIR, f"{dag_id}.py")
        with open(out_path, "w", encoding="utf-8") as f:
            f.write(template)

        os.environ["DAG_ID"] = dag_id
        os.environ["DAG_SCHEDULE"] = schedule
        os.environ["DAG_TAGS"] = tags
        os.environ["ALERT_AFTER_RUN_MINUTES"] = alert_after
        os.environ["DAG_OUTLETS"] = outlets
        jinja_utils.render_in_place(out_path)
        print(f"[ok] generated {out_path}")


def main() -> None:
    patterns = sys.argv[1:] or PATTERNS
    for p in patterns:
        build(p)


if __name__ == "__main__":
    main()
