"""CLI interface for frost."""

import argparse
import json
import logging
import sys
from pathlib import Path

from frost import __version__
from frost.config import load_config
from frost.data_loader import DataLoader
from frost.deployer import Deployer
from frost.reporter import PolicyError, report_violations
from frost.scaffold import scaffold

log = logging.getLogger("frost")


def main(argv=None):
    args = _build_parser().parse_args(argv)
    _setup_logging(verbose=args.verbose)

    # init doesn't need config
    if args.command == "init":
        _cmd_init(args)
        return

    # Load configuration
    overrides = {}
    if args.objects_folder:
        overrides["objects_folder"] = args.objects_folder
    if hasattr(args, "data_folder") and args.data_folder:
        overrides["data_folder"] = args.data_folder
    if hasattr(args, "data_schema") and args.data_schema:
        overrides["data_schema"] = args.data_schema
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
    elif args.command == "load":
        _cmd_load(config)
    elif args.command == "graph":
        _cmd_graph(config)
    else:
        log.error("Unknown command: %s", args.command)
        sys.exit(1)


# ----------------------------------------------------------------------
# Commands
# ----------------------------------------------------------------------

def _cmd_init(args):
    """Scaffold a new frost project."""
    target = args.directory
    created = scaffold(target)
    if created:
        print(f"Initialized frost project in {target}/")
        for f in created:
            print(f"  + {f}")
        print()
        print("Next steps:")
        print("  1. cp .env.example .env   # fill in Snowflake credentials")
        print("  2. frost plan             # preview execution order")
        print("  3. frost deploy           # deploy to Snowflake")
    else:
        print(f"frost project already initialized in {target}/ (no files created)")


def _cmd_plan(config):
    """Show the execution plan without deploying."""
    config.dry_run = True
    deployer = Deployer(config)
    try:
        plan = deployer.plan()
    except PolicyError as exc:
        print(report_violations(exc.violations), file=sys.stderr)
        sys.exit(1)
    print(plan)


def _cmd_deploy(config):
    """Deploy all changes to Snowflake."""
    deployer = Deployer(config)
    try:
        result = deployer.deploy()
    except PolicyError as exc:
        print(report_violations(exc.violations), file=sys.stderr)
        sys.exit(1)

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


def _cmd_load(config):
    """Load CSV data files into Snowflake."""
    from frost.connector import ConnectionConfig, SnowflakeConnector
    from frost.tracker import ChangeTracker

    loader = DataLoader(
        data_folder=config.data_folder,
        schema=config.data_schema,
    )

    data_files = loader.scan()
    if not data_files:
        print("No CSV files found in '{}'".format(config.data_folder))
        return

    if config.dry_run:
        print("Data loading plan (dry run):")
        for i, df in enumerate(data_files, 1):
            print(f"  {i}. {df.fqn}  ({len(df.columns)} cols, {len(df.rows)} rows)")
        return

    conn_cfg = ConnectionConfig(
        account=config.account,
        user=config.user,
        role=config.role,
        warehouse=config.warehouse,
        database=config.database,
        private_key_path=config.private_key_path,
        private_key_passphrase=config.private_key_passphrase,
    )
    connector = SnowflakeConnector(conn_cfg)

    loaded = 0
    failed = 0
    with connector:
        tracker = ChangeTracker(
            connector,
            database=config.database,
            tracking_schema=config.tracking_schema,
            tracking_table=config.tracking_table,
        )
        tracker.ensure_tracking_table()
        deployed_checksums = tracker.load_checksums()

        for df in data_files:
            if deployed_checksums.get(df.fqn) == df.checksum:
                log.info("SKIP  (unchanged)  %s", df.fqn)
                continue
            try:
                loader.load(connector, df)
                tracker.record_success(df.fqn, df.object_type, df.file_path, df.checksum)
                loaded += 1
            except Exception as exc:
                log.error("FAILED to load %s: %s", df.fqn, exc)
                tracker.record_failure(df.fqn, df.object_type, df.file_path, df.checksum, str(exc))
                failed += 1

    print()
    print("=" * 60)
    print("  Data Loading Summary")
    print("=" * 60)
    print(f"  Total files:  {len(data_files)}")
    print(f"  Loaded:       {loaded}")
    print(f"  Failed:       {failed}")
    print("=" * 60)

    sys.exit(0 if failed == 0 else 1)


def _cmd_graph(config):
    """Show the dependency graph."""
    deployer = Deployer(config)
    plan = deployer.plan()
    print(plan)


# ----------------------------------------------------------------------
# Argument parser
# ----------------------------------------------------------------------

def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="frost",
        description=(
            "frost -- Declarative Snowflake DDL manager with "
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

    # init
    init_parser = sub.add_parser(
        "init",
        help="Scaffold a new frost project (config, sample SQL, .env template)",
    )
    init_parser.add_argument(
        "directory",
        nargs="?",
        default=".",
        help="Target directory (default: current directory)",
    )

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

    # load
    load_parser = sub.add_parser(
        "load",
        help="Load CSV data files into Snowflake tables",
    )
    load_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be loaded without executing",
    )
    load_parser.add_argument(
        "--data-folder", "-d",
        default=None,
        help="Override data folder path (default: data/)",
    )
    load_parser.add_argument(
        "--data-schema",
        default=None,
        help="Target schema for CSV tables (default: PUBLIC)",
    )

    # graph
    graph_parser = sub.add_parser(
        "graph",
        help="Show the dependency graph",
    )

    return parser


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------

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
