"""Pattern factories. Each maps a PipelineConfig → a Dagster asset.

To add a pattern: add a module here with a `build(config) -> AssetsDefinition` function and register it
in framework/registry.py PATTERN_BUILDERS.
"""
