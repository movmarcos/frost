"""CLI interface for frost."""

import argparse
import json
import logging
import sys
from pathlib import Path

from frost import __version__
from frost.config import load_config
from frost.deployer import Deployer

log = logging.getLogger("frost")


def main(argv=None):
    args = _build_parser().parse_args(argv)
    _setup_logging(verbose=args.verbose)

    # Load configuration
    overrides = {}
    if args.objects_folder:
        overrides["objects_folder"] = args.objects_folder
    if args.verbose:
        overrides["verbose"] = True
    if hasattr(args, "dry_run") and args.dry_run:
        overrides["dry_run"] = True
    if args.vars:
        try:
            overrides["variables"] = json.loads(args.vars)
        except json.JSONDecodeError:
            log.error("--vars must be valid JSON")
            sys.exit(1)

    config = load_config(config_path=args.config, overrides=overrides)

    # Dispatch sub-command
    if args.command == "plan":
        _cmd_plan(config)
    elif args.command == "deploy":
        _cmd_deploy(config)
    elif args.command == "graph":
        _cmd_graph(config)
    else:
        log.error("Unknown command: %s", args.command)
        sys.exit(1)


# ──────────────────────────────────────────────────────────────────────
# Commands
# ──────────────────────────────────────────────────────────────────────

def _cmd_plan(config):
    """Show the execution plan without deploying."""
    config.dry_run = True
    deployer = Deployer(config)
    plan = deployer.plan()
    print(plan)


def _cmd_deploy(config):
    """Deploy all changes to Snowflake."""
    deployer = Deployer(config)
    result = deployer.deploy()

    print()
    print("=" * 60)
    print("  Deployment Summary")
    print("=" * 60)
    print(f"  Total objects:  {result.total_objects}")
    print(f"  Deployed:       {result.deployed}")
    print(f"  Skipped:        {result.skipped}")
    print(f"  Failed:         {result.failed}")
    print(f"  Elapsed:        {result.elapsed_seconds:.1f}s")
    print("=" * 60)

    if result.errors:
        print()
        print("Errors:")
        for err in result.errors:
            print(f"  - {err}")

    sys.exit(0 if result.success else 1)


def _cmd_graph(config):
    """Show the dependency graph."""
    deployer = Deployer(config)
    plan = deployer.plan()
    print(plan)


# ──────────────────────────────────────────────────────────────────────
# Argument parser
# ──────────────────────────────────────────────────────────────────────

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="frost",
        description=(
            "frost — Declarative Snowflake DDL manager with "
            "automatic dependency resolution."
        ),
    )
    parser.add_argument(
        "--version", action="version", version=f"frost {__version__}",
    )
    parser.add_argument(
        "--config", "-c",
        default="frost-config.yml",
        help="Path to config file (default: frost-config.yml)",
    )
    parser.add_argument(
        "--objects-folder", "-f",
        default=None,
        help="Override objects folder path",
    )
    parser.add_argument(
        "--vars",
        default=None,
        help='JSON string of variables, e.g. \'{"db": "MY_DB"}\'',
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose (DEBUG) logging",
    )

    sub = parser.add_subparsers(dest="command", required=True)

    # plan
    plan_parser = sub.add_parser(
        "plan",
        help="Show execution plan (parse files, resolve dependencies, show order)",
    )

    # deploy
    deploy_parser = sub.add_parser(
        "deploy",
        help="Deploy changes to Snowflake",
    )
    deploy_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be deployed without executing",
    )

    # graph
    graph_parser = sub.add_parser(
        "graph",
        help="Show the dependency graph",
    )

    return parser


# ──────────────────────────────────────────────────────────────────────
# Logging
# ──────────────────────────────────────────────────────────────────────

def _setup_logging(verbose: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    ))
    root = logging.getLogger("frost")
    root.setLevel(level)
    root.addHandler(handler)
