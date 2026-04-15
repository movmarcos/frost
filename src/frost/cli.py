"""CLI interface for frost."""

import argparse
import json
import logging
import os
import sys

from frost import __version__
from frost.config import load_config
from frost.data_loader import DataLoader
from frost.deployer import Deployer
from frost.reporter import (
    PolicyError, report_violations,
    report_deploy_errors, report_deploy_summary, report_load_summary,
    report_test_results,
)
from frost.scaffold import scaffold
from frost.streamlit import (
    discover_apps, deploy_app, teardown_app, find_snow_cli, get_app_url,
)
from frost.tester import DataTester
from frost.visualizer import edges_from_rows, generate_html, write_and_open

log = logging.getLogger("frost")


def main(argv=None):
    args = _build_parser().parse_args(argv)
    json_mode = getattr(args, "json", False)
    _setup_logging(verbose=args.verbose, json_mode=json_mode)

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
    if hasattr(args, "no_cortex") and args.no_cortex:
        overrides["cortex"] = False
    if hasattr(args, "cortex_model") and args.cortex_model:
        overrides["cortex_model"] = args.cortex_model
    if hasattr(args, "force") and args.force:
        overrides["force"] = True
    if hasattr(args, "target") and args.target:
        overrides["target"] = args.target
    if args.vars:
        try:
            overrides["variables"] = json.loads(args.vars)
        except json.JSONDecodeError:
            log.error("--vars must be valid JSON")
            sys.exit(1)

    config = load_config(config_path=args.config, overrides=overrides)

    # Dispatch sub-command
    if args.command == "plan":
        _cmd_plan(config, args)
    elif args.command == "deploy":
        _cmd_deploy(config)
    elif args.command == "load":
        _cmd_load(config, args)
    elif args.command == "graph":
        _cmd_graph(config, args)
    elif args.command == "lineage":
        _cmd_lineage(config, args)
    elif args.command == "test":
        _cmd_test(config, args)
    elif args.command == "streamlit":
        _cmd_streamlit(config, args)
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


def _cmd_plan(config, args):
    """Show the execution plan without deploying."""
    config.dry_run = True
    deployer = Deployer(config)
    try:
        plan = deployer.plan()
    except PolicyError as exc:
        print(report_violations(exc.violations), file=sys.stderr)
        sys.exit(1)

    if getattr(args, "json", False):
        deployer._scan_and_parse()
        deployer._build_graph()
        ordered = deployer._graph.resolve_order()
        payload = {
            "objects": [
                {
                    "fqn": obj.fqn,
                    "object_type": obj.object_type,
                    "file_path": obj.file_path,
                    "dependencies": sorted(obj.dependencies),
                    "columns": obj.columns,
                    "checksum": obj.checksum,
                }
                for obj in ordered
            ],
            "total": len(ordered),
        }
        print(json.dumps(payload, indent=2))
    else:
        print(plan)


def _cmd_deploy(config):
    """Deploy all changes to Snowflake."""
    deployer = Deployer(config)
    try:
        result = deployer.deploy()
    except PolicyError as exc:
        print(report_violations(exc.violations), file=sys.stderr)
        sys.exit(1)

    # Rich deployment errors
    if result.deploy_errors:
        print(report_deploy_errors(result.deploy_errors), file=sys.stderr)

    # Branded summary
    print(report_deploy_summary(
        total=result.total_objects,
        deployed=result.deployed,
        skipped=result.skipped,
        failed=result.failed,
        elapsed=result.elapsed_seconds,
    ))

    sys.exit(0 if result.success else 1)


def _cmd_load(config, args):
    """Load CSV data files into Snowflake."""
    from frost.connector import ConnectionConfig, SnowflakeConnector
    from frost.tracker import ChangeTracker

    json_mode = getattr(args, "json", False)
    if json_mode:
        _setup_logging(json_mode=True)

    loader = DataLoader(
        data_folder=config.data_folder,
        schema=config.data_schema,
    )

    data_files = loader.scan()
    if not data_files:
        if json_mode:
            print(json.dumps({"files": []}))
        else:
            print("No CSV files found in '{}'".format(config.data_folder))
        return

    if json_mode:
        items = []
        for df in data_files:
            items.append({
                "fqn": df.fqn,
                "file_path": df.file_path,
                "table_name": df.table_name,
                "schema": df.schema or "",
                "columns": df.columns,
                "column_types": df.column_types,
                "row_count": len(df.rows),
                "checksum": df.checksum,
            })
        print(json.dumps({"files": items}, indent=2))
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

    print(report_load_summary(
        total=len(data_files),
        loaded=loaded,
        failed=failed,
    ))

    sys.exit(0 if failed == 0 else 1)


def _cmd_graph(config, args):
    """Show the dependency graph."""
    deployer = Deployer(config)
    violations = []

    try:
        plan = deployer.plan()
    except PolicyError as exc:
        # Objects are already parsed; build the graph anyway for JSON output
        violations = exc.violations if hasattr(exc, "violations") else []
        try:
            deployer._build_graph()
            plan = deployer._graph.visualize()
        except Exception:
            plan = ""

    if getattr(args, "json", False):
        objects = deployer._graph.resolve_order()
        node_types = deployer._graph.get_node_types()
        node_columns = deployer._graph.get_node_columns()
        edges = deployer._graph.get_all_edges()
        payload = {
            "nodes": [
                {
                    "fqn": obj.fqn,
                    "object_type": obj.object_type,
                    "file_path": obj.file_path,
                    "schema": obj.schema or "",
                    "name": obj.name,
                    "columns": obj.columns,
                    "dependencies": sorted(obj.dependencies),
                }
                for obj in objects
            ],
            "edges": edges,
            "node_types": node_types,
            "node_columns": node_columns,
            "violations": [str(v) for v in violations],
        }
        print(json.dumps(payload, indent=2))
    else:
        if violations:
            print(report_violations(violations))
        print(plan)


def _cmd_lineage(config, args):
    """Generate an interactive HTML lineage visualisation or JSON payload."""
    output = getattr(args, "output", "lineage.html")
    local = getattr(args, "local", False)
    focus_object = getattr(args, "object", None)
    initial_depth = getattr(args, "depth", 1)
    direction = getattr(args, "direction", "both")
    json_mode = getattr(args, "json", False)

    # JSON mode always implies --local; querying Snowflake for subgraphs
    # is not a supported use case in Phase 1.
    if json_mode and not local:
        log.error("--json currently requires --local")
        sys.exit(2)

    if json_mode:
        _cmd_lineage_json(config, focus_object, initial_depth, direction)
        return

    # Existing HTML path -- unchanged behaviour.
    if local:
        from frost.deployer import Deployer
        deployer = Deployer(config)
        try:
            deployer._scan_and_parse()
        except PolicyError:
            pass
        deployer._build_graph()
        edges = deployer._graph.get_all_edges()
        node_types = deployer._graph.get_node_types()
        node_columns = deployer._graph.get_node_columns()
        if not edges:
            print("No edges found -- nothing to visualise.")
            return
        html = generate_html(edges, title="frost · Lineage (local)",
                             focus_object=focus_object,
                             node_types=node_types,
                             initial_depth=initial_depth,
                             node_columns=node_columns)
    else:
        # Remote Snowflake path -- unchanged from current implementation.
        from frost.connector import ConnectionConfig, SnowflakeConnector
        conn_cfg = ConnectionConfig(
            account=config.account,
            user=config.user,
            role=config.role,
            warehouse=config.warehouse,
            database=config.database,
            private_key_path=config.private_key_path,
            private_key_passphrase=config.private_key_passphrase,
        )
        schema = config.tracking_schema or "FROST"
        table = f"{schema}.OBJECT_LINEAGE"
        connector = SnowflakeConnector(conn_cfg)
        with connector:
            rows = connector.execute(f"SELECT * FROM {table} ORDER BY object_fqn")
            if not rows:
                print(f"No lineage data in {table} -- run 'frost deploy' first.")
                return
            edges = edges_from_rows(rows)

            history_schema = config.tracking_schema or "FROST"
            history_table = f"{history_schema}.DEPLOY_HISTORY"
            type_rows = connector.execute(f"""
                SELECT object_fqn, object_type
                FROM {history_table}
                WHERE status = 'SUCCESS'
                QUALIFY ROW_NUMBER() OVER (
                    PARTITION BY object_fqn ORDER BY deployed_at DESC
                ) = 1
            """)
            node_types = {r[0]: r[1] for r in type_rows} if type_rows else {}

            node_columns: dict = {}
            try:
                col_rows = connector.execute(f"""
                    SELECT TABLE_CATALOG || '.' || TABLE_SCHEMA || '.' || TABLE_NAME,
                           COLUMN_NAME,
                           DATA_TYPE
                    FROM {config.database}.INFORMATION_SCHEMA.COLUMNS
                    ORDER BY TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, ORDINAL_POSITION
                """)
                if col_rows:
                    for r in col_rows:
                        fqn3 = r[0].upper()
                        col = {"name": r[1], "type": r[2]}
                        node_columns.setdefault(fqn3, []).append(col)
                        parts = fqn3.split('.', 1)
                        fqn2 = parts[1] if len(parts) == 2 else fqn3
                        node_columns.setdefault(fqn2, []).append(col)
            except Exception as exc:
                print(f"Warning: could not fetch column metadata: {exc}")

        html = generate_html(edges, title="frost · Lineage",
                             focus_object=focus_object,
                             node_types=node_types,
                             initial_depth=initial_depth,
                             node_columns=node_columns)

    path = write_and_open(html, output)
    print(f"Lineage visual opened: {path}")


def _cmd_lineage_json(config, focus_object, depth, direction):
    """Emit a subgraph or full-graph JSON payload to stdout (no Snowflake)."""
    from frost.deployer import Deployer
    from frost.graph import extract_subgraph
    from frost.visualizer import nodes_and_edges_as_json

    deployer = Deployer(config)
    try:
        deployer._scan_and_parse()
    except PolicyError:
        pass
    deployer._build_graph()
    graph = deployer._graph

    if focus_object:
        subset = extract_subgraph(
            graph, focus_object, depth=depth, direction=direction,
        )
        if subset is None:
            print(json.dumps({
                "error": "object not found",
                "fqn": focus_object.upper(),
            }))
            sys.exit(2)
        payload = nodes_and_edges_as_json(
            nodes=subset.nodes,
            edges=subset.edges,
            focus=subset.focus,
            depth=subset.depth,
            direction=subset.direction,
            truncated=subset.truncated,
        )
    else:
        # Full-graph JSON: reuse existing edge/node gathering logic.
        edges = graph.get_all_edges()
        node_types = graph.get_node_types()
        node_columns = graph.get_node_columns()
        fqns = set(node_types) | {e["source"] for e in edges} | {e["target"] for e in edges}
        nodes = []
        for fqn in sorted(fqns):
            nodes.append({
                "fqn": fqn,
                "object_type": node_types.get(fqn, "EXTERNAL"),
                "file_path": (
                    graph._objects[fqn].file_path if fqn in graph._objects else ""
                ),
                "columns": node_columns.get(fqn, []),
            })
        payload = nodes_and_edges_as_json(
            nodes=nodes,
            edges=edges,
            focus=None, depth=None, direction=None, truncated=False,
        )

    print(json.dumps(payload))


def _cmd_test(config, args):
    """Run YAML-defined data quality tests against CSV files."""
    data_folder = getattr(args, "data_folder", None) or config.data_folder
    target = getattr(args, "name", None)

    tester = DataTester(data_folder=data_folder, target=target)

    # Validate unique basenames
    dup_errors = tester.validate_unique_basenames()
    if dup_errors:
        print("Error: duplicate file names in data folder (names must be unique):")
        for err in dup_errors:
            print(f"  {err}")
        sys.exit(1)

    cases = tester.load_tests()
    if not cases:
        label = f"'{target}'" if target else f"'{data_folder}'"
        print(f"No tests found in {label}")
        return

    log.info("Running %d data test(s) from '%s'", len(cases), data_folder)
    results = tester.run(cases)

    passed = sum(1 for r in results if r.passed)
    failed = sum(1 for r in results if not r.passed)

    print(report_test_results(results))
    sys.exit(0 if failed == 0 else 1)


def _cmd_streamlit(config, args):
    """Manage Streamlit apps via Snowflake CLI (snow)."""
    action = args.action
    json_mode = getattr(args, "json", False)
    if json_mode:
        _setup_logging(json_mode=True)

    # Discover apps from snowflake.yml files
    # Use the directory containing the config file as project root
    config_path = os.path.abspath(getattr(args, "config", "frost-config.yml"))
    project_root = os.path.dirname(config_path)
    apps = discover_apps(project_root)

    if action == "list":
        if not apps:
            if json_mode:
                print(json.dumps({"apps": [], "snow_cli": find_snow_cli() or ""}))
            else:
                print("No Streamlit apps found (no snowflake.yml with streamlit definitions).")
            return

        if json_mode:
            payload = {
                "apps": [a.to_dict() for a in apps],
                "snow_cli": find_snow_cli() or "",
            }
            print(json.dumps(payload, indent=2))
        else:
            snow = find_snow_cli()
            print(f"\n{'─' * 60}")
            print(f"  ❄  Streamlit Apps  ({len(apps)} found)")
            print(f"{'─' * 60}")
            for app in apps:
                print(f"\n  {app.name}")
                print(f"    Main file : {app.main_file}")
                print(f"    Directory : {app.directory}")
                print(f"    Schema    : {app.schema}")
                print(f"    Stage     : {app.stage}")
                if app.warehouse:
                    print(f"    Warehouse : {app.warehouse}")
                print(f"    Config    : {app.definition_file}")
            print()
            if snow:
                print(f"  snow CLI: {snow}")
            else:
                print("  ⚠  snow CLI not found — install: pip install snowflake-cli-labs")
            print()
        return

    elif action == "deploy":
        snow = find_snow_cli()
        if not snow:
            log.error(
                "Snowflake CLI (snow) not found. "
                "Install: pip install snowflake-cli-labs  or  brew install snowflake-cli"
            )
            sys.exit(1)

        target_name = getattr(args, "name", None)
        if target_name:
            matching = [a for a in apps if a.name == target_name]
            if not matching:
                log.error("Streamlit app '%s' not found. Available: %s",
                          target_name, ", ".join(a.name for a in apps))
                sys.exit(1)
            targets = matching
        else:
            targets = apps

        if not targets:
            log.error("No Streamlit apps to deploy.")
            sys.exit(1)

        connection = getattr(args, "connection", None)
        replace_flag = getattr(args, "replace", True)
        open_flag = getattr(args, "open", False)

        results = []
        for app in targets:
            result = deploy_app(
                app, snow,
                replace=replace_flag,
                open_browser=open_flag,
                connection=connection,
            )
            results.append(result)

        if json_mode:
            print(json.dumps({
                "results": [
                    {
                        "name": r.name,
                        "success": r.success,
                        "message": r.message,
                        "url": r.url,
                    }
                    for r in results
                ]
            }, indent=2))
        else:
            ok = sum(1 for r in results if r.success)
            fail = sum(1 for r in results if not r.success)
            print(f"\n{'─' * 60}")
            print(f"  ❄  Streamlit Deploy  ({ok} succeeded, {fail} failed)")
            print(f"{'─' * 60}")
            for r in results:
                icon = "✓" if r.success else "✗"
                print(f"\n  {icon}  {r.name}")
                if r.url:
                    print(f"     URL: {r.url}")
                if not r.success:
                    print(f"     Error: {r.message}")
            print()

        sys.exit(0 if all(r.success for r in results) else 1)

    elif action == "teardown":
        snow = find_snow_cli()
        if not snow:
            log.error("Snowflake CLI (snow) not found.")
            sys.exit(1)

        target_name = getattr(args, "name", None)
        if not target_name:
            log.error("Please specify the app name to tear down: frost streamlit teardown <name>")
            sys.exit(1)

        matching = [a for a in apps if a.name == target_name]
        if not matching:
            log.error("Streamlit app '%s' not found.", target_name)
            sys.exit(1)

        connection = getattr(args, "connection", None)
        result = teardown_app(matching[0], snow, connection=connection)

        if json_mode:
            print(json.dumps({
                "name": result.name,
                "success": result.success,
                "message": result.message,
            }, indent=2))
        else:
            if result.success:
                print(f"Streamlit app '{result.name}' torn down successfully.")
            else:
                print(f"Failed to tear down '{result.name}': {result.message}")

        sys.exit(0 if result.success else 1)

    elif action == "get-url":
        snow = find_snow_cli()
        if not snow:
            log.error("Snowflake CLI (snow) not found.")
            sys.exit(1)

        target_name = getattr(args, "name", None)
        if not target_name:
            log.error("Please specify the app name: frost streamlit get-url <name>")
            sys.exit(1)

        matching = [a for a in apps if a.name == target_name]
        if not matching:
            log.error("Streamlit app '%s' not found.", target_name)
            sys.exit(1)

        connection = getattr(args, "connection", None)
        url = get_app_url(matching[0], snow, connection=connection)

        if json_mode:
            print(json.dumps({"name": target_name, "url": url or ""}))
        else:
            if url:
                print(url)
            else:
                print(f"Could not get URL for '{target_name}'.")

        sys.exit(0 if url else 1)

    else:
        log.error("Unknown streamlit action: %s", action)
        log.error("Available: list, deploy, teardown, get-url")
        sys.exit(1)


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
    plan_parser.add_argument(
        "--json",
        action="store_true",
        help="Output execution plan as JSON (for tooling integration)",
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
    deploy_parser.add_argument(
        "--force",
        action="store_true",
        default=False,
        help="Redeploy all objects regardless of checksum (ignore change tracking)",
    )
    deploy_parser.add_argument(
        "--target",
        default=None,
        metavar="FQN",
        help="Redeploy a specific object (and its dependents), e.g. PUBLIC.MY_VIEW",
    )
    deploy_parser.add_argument(
        "--no-cortex",
        action="store_true",
        default=False,
        help="Disable Cortex AI fix suggestions on errors",
    )
    deploy_parser.add_argument(
        "--cortex-model",
        default=None,
        help="Cortex LLM model for fix suggestions (default: mistral-large2)",
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
    load_parser.add_argument(
        "--json",
        action="store_true",
        help="Output data file info as JSON (for tooling integration)",
    )

    # graph
    graph_parser = sub.add_parser(
        "graph",
        help="Show the dependency graph",
    )
    graph_parser.add_argument(
        "--json",
        action="store_true",
        help="Output graph as JSON (nodes, edges, columns, types)",
    )

    # lineage
    lineage_parser = sub.add_parser(
        "lineage",
        help="Generate an interactive HTML lineage visualisation",
    )
    lineage_parser.add_argument(
        "--output", "-o",
        default="lineage.html",
        help="Output HTML file path (default: lineage.html)",
    )
    lineage_parser.add_argument(
        "--local",
        action="store_true",
        help="Build from local SQL files instead of querying Snowflake",
    )
    lineage_parser.add_argument(
        "--object",
        default=None,
        metavar="FQN",
        help="Focus lineage on a specific object (e.g. PUBLIC.MY_TABLE)",
    )
    lineage_parser.add_argument(
        "--depth",
        type=int,
        default=1,
        metavar="N",
        help="Default neighbourhood depth when clicking a node (default: 1)",
    )
    lineage_parser.add_argument(
        "--direction",
        choices=["up", "down", "both"],
        default="both",
        help="Subgraph traversal direction when used with --object --json "
             "(default: both)",
    )
    lineage_parser.add_argument(
        "--json",
        action="store_true",
        help="Output lineage as JSON (enables subgraph mode when combined "
             "with --object)",
    )

    # test
    test_parser = sub.add_parser(
        "test",
        help="Run data quality tests defined in YAML sidecars against CSV files",
    )
    test_parser.add_argument(
        "name",
        nargs="?",
        default=None,
        help="File name (without extension) to test; omit to run all tests",
    )
    test_parser.add_argument(
        "--data-folder", "-d",
        default=None,
        help="Override data folder path (default: data/)",
    )

    # streamlit
    st_parser = sub.add_parser(
        "streamlit",
        help="Manage Streamlit apps via Snowflake CLI (snow)",
    )
    st_parser.add_argument(
        "action",
        choices=["list", "deploy", "teardown", "get-url"],
        help="Action to perform (list, deploy, teardown, get-url)",
    )
    st_parser.add_argument(
        "name",
        nargs="?",
        default=None,
        help="App name (required for teardown/get-url, optional for deploy)",
    )
    st_parser.add_argument(
        "--json",
        action="store_true",
        help="Output as JSON (for tooling integration)",
    )
    st_parser.add_argument(
        "--replace",
        action="store_true",
        default=True,
        help="Replace existing Streamlit app on deploy (default: true)",
    )
    st_parser.add_argument(
        "--open",
        action="store_true",
        default=False,
        help="Open the app in browser after deploy",
    )
    st_parser.add_argument(
        "--connection",
        default=None,
        help="Named Snowflake CLI connection to use",
    )

    return parser


# ----------------------------------------------------------------------
# Logging
# ----------------------------------------------------------------------

def _setup_logging(verbose: bool = False, json_mode: bool = False):
    level = logging.DEBUG if verbose else logging.INFO
    # When --json is active, send logs to stderr so stdout is pure JSON
    stream = sys.stderr if json_mode else sys.stdout
    handler = logging.StreamHandler(stream)
    handler.setFormatter(logging.Formatter(
        "%(asctime)s  %(levelname)-7s  %(message)s",
        datefmt="%H:%M:%S",
    ))
    root = logging.getLogger("frost")
    root.setLevel(level)
    root.addHandler(handler)
