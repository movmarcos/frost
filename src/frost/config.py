"""Configuration loader -- merges YAML config with environment variables."""

import json
import logging
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, Optional

import yaml

log = logging.getLogger("frost")

_DEFAULT_CONFIG_NAME = "frost-config.yml"


@dataclass
class FrostConfig:
    """Resolved runtime configuration."""

    # Snowflake connection
    account: str = ""
    user: str = ""
    role: str = "SYSADMIN"
    warehouse: str = "COMPUTE_WH"
    database: Optional[str] = None
    private_key_path: str = ""
    private_key_passphrase: Optional[str] = None

    # Frost settings
    objects_folder: str = "objects"
    data_folder: str = "data"
    tracking_schema: str = "FROST"
    tracking_table: str = "DEPLOY_HISTORY"

    # Variables for SQL substitution
    variables: Dict[str, str] = field(default_factory=dict)

    # Runtime flags
    dry_run: bool = False
    verbose: bool = False
    plan_only: bool = False


def load_config(
    config_path: Optional[str] = None,
    overrides: Optional[Dict] = None,
) -> FrostConfig:
    """Load config from YAML file, then overlay environment variables.

    Priority (highest -> lowest):
      1. CLI flags (overrides dict)
      2. Environment variables
      3. YAML config file
      4. Defaults
    """
    cfg = FrostConfig()

    # -- 1. YAML file -------------------------------------------------
    if config_path is None:
        config_path = _DEFAULT_CONFIG_NAME
    path = Path(config_path)

    if path.is_file():
        log.debug("Loading config from %s", path)
        with open(path) as fh:
            data = yaml.safe_load(fh) or {}
        _apply_dict(cfg, data)
    else:
        log.debug("No config file at %s -- using defaults + env", path)

    # -- 2. Environment variables --------------------------------------
    _env = os.environ.get
    cfg.account                = _env("SNOWFLAKE_ACCOUNT",                cfg.account)
    cfg.user                   = _env("SNOWFLAKE_USER",                   cfg.user)
    cfg.role                   = _env("SNOWFLAKE_ROLE",                   cfg.role)
    cfg.warehouse              = _env("SNOWFLAKE_WAREHOUSE",              cfg.warehouse)
    cfg.database               = _env("SNOWFLAKE_DATABASE",               cfg.database)
    cfg.private_key_path       = _env("SNOWFLAKE_PRIVATE_KEY_PATH",       cfg.private_key_path)
    cfg.private_key_passphrase = _env("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", cfg.private_key_passphrase)

    cfg.data_folder              = _env("FROST_DATA_FOLDER",              cfg.data_folder)

    vars_json = _env("FROST_VARS", None)
    if vars_json:
        try:
            cfg.variables.update(json.loads(vars_json))
        except json.JSONDecodeError:
            log.warning("FROST_VARS is not valid JSON -- ignoring")

    # -- 3. CLI overrides ----------------------------------------------
    if overrides:
        _apply_dict(cfg, overrides)

    return cfg


def _apply_dict(cfg: FrostConfig, data: dict) -> None:
    """Set config fields from a dict (YAML or CLI overrides)."""
    mapping = {
        "account":              "account",
        "user":                 "user",
        "role":                 "role",
        "warehouse":            "warehouse",
        "database":             "database",
        "private_key_path":     "private_key_path",
        "private_key_passphrase": "private_key_passphrase",
        "objects_folder":       "objects_folder",
        "data_folder":          "data_folder",
        "tracking_schema":      "tracking_schema",
        "tracking_table":       "tracking_table",
        "dry_run":              "dry_run",
        "verbose":              "verbose",
        "plan_only":            "plan_only",
    }
    for key, attr in mapping.items():
        # Support both snake_case and kebab-case keys in YAML
        val = data.get(key) or data.get(key.replace("_", "-"))
        if val is not None:
            setattr(cfg, attr, val)

    # Variables
    variables = data.get("variables") or data.get("vars")
    if isinstance(variables, dict):
        cfg.variables.update(variables)
    elif isinstance(variables, str):
        try:
            cfg.variables.update(json.loads(variables))
        except json.JSONDecodeError:
            pass
