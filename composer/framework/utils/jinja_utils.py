"""Build-time Jinja rendering for generated DAG files.

Templates use a custom `env_override` filter so the same template renders both:
  - at build time, where DAG_ID/DAG_SCHEDULE/etc. come from os.environ, and
  - at runtime in Airflow, where the same filter resolves Airflow env vars.

`{% raw %}...{% endraw %}` blocks in templates protect Airflow's own
`{{ task_instance.xcom_pull(...) }}` macros from being rendered at build time.
"""
import os

from jinja2 import Environment, FileSystemLoader


def _env_override(value, key):
    """Return os.getenv(key) or the provided default. Mirrors runtime common_utils.env_override."""
    return os.getenv(key, value)


def render_in_place(file_path: str) -> None:
    directory, filename = os.path.split(file_path)
    env = Environment(loader=FileSystemLoader(directory or "."))
    env.filters["env_override"] = _env_override

    # Also expose as `common_utils.env_override` to match the reference template syntax.
    class _NS:
        env_override = staticmethod(_env_override)

    env.globals["common_utils"] = _NS

    template = env.get_template(filename)
    rendered = template.render()
    with open(file_path, "w", encoding="utf-8") as f:
        f.write(rendered)
