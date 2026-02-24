"""Tests for frost.config -- YAML loading, env vars, overrides."""

import json
import os
import pytest

from frost.config import FrostConfig, load_config


# ------------------------------------------------------------------
# Defaults
# ------------------------------------------------------------------

def test_defaults():
    """FrostConfig should have sensible defaults."""
    cfg = FrostConfig()
    assert cfg.role == "SYSADMIN"
    assert cfg.warehouse == "COMPUTE_WH"
    assert cfg.objects_folder == "objects"
    assert cfg.data_folder == "data"
    assert cfg.data_schema == "PUBLIC"
    assert cfg.tracking_schema == "FROST"
    assert cfg.tracking_table == "DEPLOY_HISTORY"
    assert cfg.cortex is True
    assert cfg.cortex_model == "mistral-large2"
    assert cfg.dry_run is False
    assert cfg.verbose is False


# ------------------------------------------------------------------
# YAML loading
# ------------------------------------------------------------------

def test_load_from_yaml(tmp_path):
    """load_config should read values from a YAML file."""
    config_file = tmp_path / "frost-config.yml"
    config_file.write_text(
        "account: my_account\n"
        "user: my_user\n"
        "role: ANALYST\n"
        "warehouse: SMALL_WH\n"
        "database: MY_DB\n"
        "objects-folder: sql\n"
    )
    cfg = load_config(config_path=str(config_file))

    assert cfg.account == "my_account"
    assert cfg.user == "my_user"
    assert cfg.role == "ANALYST"
    assert cfg.warehouse == "SMALL_WH"
    assert cfg.database == "MY_DB"
    assert cfg.objects_folder == "sql"


def test_load_variables_from_yaml(tmp_path):
    """Variables in YAML should be loaded into cfg.variables."""
    config_file = tmp_path / "frost-config.yml"
    config_file.write_text(
        "variables:\n"
        "  env: PROD\n"
        "  schema: ANALYTICS\n"
    )
    cfg = load_config(config_path=str(config_file))
    assert cfg.variables == {"env": "PROD", "schema": "ANALYTICS"}


def test_missing_yaml_uses_defaults(tmp_path):
    """If the YAML file doesn't exist, defaults are used."""
    cfg = load_config(config_path=str(tmp_path / "nonexistent.yml"))
    assert cfg.role == "SYSADMIN"


# ------------------------------------------------------------------
# Environment variables
# ------------------------------------------------------------------

def test_env_var_overrides(monkeypatch):
    """Environment variables should override YAML defaults."""
    monkeypatch.setenv("SNOWFLAKE_ACCOUNT", "env_account")
    monkeypatch.setenv("SNOWFLAKE_USER", "env_user")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "ENV_ROLE")
    monkeypatch.setenv("SNOWFLAKE_WAREHOUSE", "ENV_WH")
    monkeypatch.setenv("SNOWFLAKE_DATABASE", "ENV_DB")

    cfg = load_config(config_path="/nonexistent.yml")

    assert cfg.account == "env_account"
    assert cfg.user == "env_user"
    assert cfg.role == "ENV_ROLE"
    assert cfg.warehouse == "ENV_WH"
    assert cfg.database == "ENV_DB"


def test_frost_vars_json(monkeypatch):
    """FROST_VARS env var should parse JSON into variables."""
    monkeypatch.setenv("FROST_VARS", '{"db": "MY_DB", "role": "ADMIN"}')
    cfg = load_config(config_path="/nonexistent.yml")
    assert cfg.variables == {"db": "MY_DB", "role": "ADMIN"}


def test_frost_vars_invalid_json(monkeypatch):
    """Invalid JSON in FROST_VARS should be silently ignored."""
    monkeypatch.setenv("FROST_VARS", "not-json")
    cfg = load_config(config_path="/nonexistent.yml")
    assert cfg.variables == {}


def test_cortex_env_disabled(monkeypatch):
    """FROST_CORTEX=false should disable cortex."""
    monkeypatch.setenv("FROST_CORTEX", "false")
    cfg = load_config(config_path="/nonexistent.yml")
    assert cfg.cortex is False


def test_cortex_env_enabled(monkeypatch):
    """FROST_CORTEX=1 should enable cortex."""
    monkeypatch.setenv("FROST_CORTEX", "1")
    cfg = load_config(config_path="/nonexistent.yml")
    assert cfg.cortex is True


# ------------------------------------------------------------------
# CLI overrides (highest priority)
# ------------------------------------------------------------------

def test_cli_overrides_yaml_and_env(tmp_path, monkeypatch):
    """CLI overrides should take highest priority."""
    config_file = tmp_path / "frost-config.yml"
    config_file.write_text("role: YAML_ROLE\n")
    monkeypatch.setenv("SNOWFLAKE_ROLE", "ENV_ROLE")

    cfg = load_config(
        config_path=str(config_file),
        overrides={"role": "CLI_ROLE"},
    )
    assert cfg.role == "CLI_ROLE"


def test_override_dry_run():
    """dry_run override should apply."""
    cfg = load_config(
        config_path="/nonexistent.yml",
        overrides={"dry_run": True},
    )
    assert cfg.dry_run is True


# ------------------------------------------------------------------
# Kebab-case support
# ------------------------------------------------------------------

def test_kebab_case_yaml(tmp_path):
    """YAML keys with kebab-case should work."""
    config_file = tmp_path / "frost-config.yml"
    config_file.write_text(
        "private-key-path: /path/to/key.p8\n"
        "tracking-schema: CUSTOM\n"
        "tracking-table: CUSTOM_TABLE\n"
        "data-folder: my_data\n"
        "data-schema: STAGING\n"
    )
    cfg = load_config(config_path=str(config_file))
    assert cfg.private_key_path == "/path/to/key.p8"
    assert cfg.tracking_schema == "CUSTOM"
    assert cfg.tracking_table == "CUSTOM_TABLE"
    assert cfg.data_folder == "my_data"
    assert cfg.data_schema == "STAGING"
