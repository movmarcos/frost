"""frost — Declarative Snowflake DDL manager with automatic dependency resolution."""

from importlib.metadata import version, PackageNotFoundError

try:
    __version__ = version("frost-ddl")
except PackageNotFoundError:
    __version__ = "0.1.0"  # fallback for development
