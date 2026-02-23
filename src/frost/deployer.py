"""Deployer -- the orchestration engine.

Ties together the parser, graph, connector, and tracker to:
  1. Scan SQL files
  2. Parse objects & dependencies
  3. Build the dependency graph
  4. Determine what changed (checksum comparison)
  5. Cascade: mark changed objects + all their dependents for re-deploy
  6. Execute in topological order
"""

import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

import snowflake.connector

from frost.config import FrostConfig
from frost.connector import ConnectionConfig, SnowflakeConnector
from frost.cortex import enrich_errors_with_cortex
from frost.graph import CycleError, DependencyGraph
from frost.parser import ObjectDefinition, SqlParser
from frost.reporter import DeployError, PolicyError, report_violations
from frost.tracker import ChangeTracker

log = logging.getLogger("frost")


# ----------------------------------------------------------------------
# Result model
# ----------------------------------------------------------------------

@dataclass
class DeployResult:
    total_objects: int = 0
    deployed: int = 0
    skipped: int = 0
    failed: int = 0
    errors: List[str] = field(default_factory=list)
    deploy_errors: List[DeployError] = field(default_factory=list)
    execution_order: List[str] = field(default_factory=list)
    elapsed_seconds: float = 0.0

    @property
    def success(self) -> bool:
        return self.failed == 0


# ----------------------------------------------------------------------
# Deployer
# ----------------------------------------------------------------------

class Deployer:
    """Main deployment orchestrator."""

    def __init__(self, config: FrostConfig):
        self.config = config
        self._parser = SqlParser(variables=config.variables)
        self._graph = DependencyGraph()
        self._objects: Dict[str, ObjectDefinition] = {}

    # -- public API ----------------------------------------------------

    def plan(self) -> str:
        """Parse all files, build the graph, and return the execution plan."""
        self._scan_and_parse()
        self._build_graph()
        return self._graph.visualize()

    def deploy(self) -> DeployResult:
        """Full deployment: parse -> graph -> diff -> execute."""
        t0 = time.time()
        result = DeployResult()

        # 1. Parse
        self._scan_and_parse()
        result.total_objects = len(self._objects)

        if result.total_objects == 0:
            log.warning("No SQL objects found in '%s'", self.config.objects_folder)
            return result

        # 2. Build graph
        self._build_graph()

        try:
            ordered = self._graph.resolve_order()
        except CycleError as exc:
            log.error("%s", exc)
            result.errors.append(str(exc))
            result.failed = result.total_objects
            return result

        result.execution_order = [obj.fqn for obj in ordered]

        # 3. Connect to Snowflake
        conn_cfg = ConnectionConfig(
            account=self.config.account,
            user=self.config.user,
            role=self.config.role,
            warehouse=self.config.warehouse,
            database=self.config.database,
            private_key_path=self.config.private_key_path,
            private_key_passphrase=self.config.private_key_passphrase,
        )

        if self.config.dry_run:
            log.info("DRY RUN -- no changes will be applied")
            self._dry_run(ordered, result)
            result.elapsed_seconds = time.time() - t0
            return result

        connector = SnowflakeConnector(conn_cfg)
        with connector:
            tracker = ChangeTracker(
                connector,
                tracking_schema=self.config.tracking_schema,
                tracking_table=self.config.tracking_table,
            )
            tracker.ensure_tracking_table()
            deployed_checksums = tracker.load_checksums()

            # 4. Determine what changed
            current_checksums = {fqn: obj.checksum for fqn, obj in self._objects.items()}
            changed_fqns = tracker.get_changed_fqns(current_checksums)

            # 5. Cascade: also redeploy dependents of changed objects
            to_deploy: Set[str] = set()
            for fqn in changed_fqns:
                to_deploy.add(fqn)
                to_deploy.update(self._graph.get_dependents(fqn))

            log.info(
                "Objects: %d total, %d changed, %d to deploy (with cascaded dependents)",
                len(ordered), len(changed_fqns), len(to_deploy),
            )

            # 6. Execute in order
            failed_fqns: Set[str] = set()
            for obj in ordered:
                if obj.fqn not in to_deploy:
                    log.info("SKIP  (unchanged)  %s", obj.fqn)
                    result.skipped += 1
                    continue

                # Check if any dependency failed
                blocked_by = obj.dependencies & failed_fqns
                if blocked_by:
                    log.warning(
                        "SKIP  (blocked)  %s  -- depends on failed: %s",
                        obj.fqn, ", ".join(sorted(blocked_by)),
                    )
                    failed_fqns.add(obj.fqn)
                    result.failed += 1
                    result.skipped += 1
                    # Add this FQN to the blocked list of the error that caused it
                    for de in result.deploy_errors:
                        if de.fqn in blocked_by:
                            de.blocked.append(obj.fqn)
                            break
                    continue

                log.info("DEPLOY  [%s]  %s", obj.object_type, obj.fqn)
                try:
                    connector.execute(obj.resolved_sql)
                except Exception as exc:
                    # ---- SQL execution failed on Snowflake ----
                    err_code = None
                    if isinstance(exc, snowflake.connector.Error):
                        err_code = str(getattr(exc, 'errno', '') or '').zfill(6) if getattr(exc, 'errno', None) else None
                        err_msg = getattr(exc, 'msg', '') or str(exc)
                    else:
                        err_msg = str(exc)

                    log.error("  FAILED: %s", err_msg)
                    try:
                        tracker.record_failure(
                            obj.fqn, obj.object_type, obj.file_path, obj.checksum,
                            error=err_msg, sql=obj.resolved_sql,
                        )
                    except Exception as track_exc:
                        log.warning("  Could not record failure in tracking table: %s", track_exc)

                    failed_fqns.add(obj.fqn)
                    result.failed += 1
                    result.errors.append(f"{obj.fqn}: {err_msg}")
                    result.deploy_errors.append(DeployError(
                        fqn=obj.fqn,
                        object_type=obj.object_type,
                        file_path=obj.file_path,
                        sql=obj.resolved_sql,
                        error_message=err_msg,
                        error_code=err_code,
                    ))
                    continue

                # ---- SQL executed OK -- record in tracking table ----
                try:
                    tracker.record_success(
                        obj.fqn, obj.object_type, obj.file_path, obj.checksum,
                        sql=obj.resolved_sql,
                    )
                except Exception as track_exc:
                    log.warning(
                        "  Deployed OK but could not record in tracking table: %s",
                        track_exc,
                    )
                result.deployed += 1
                log.info("  OK")

            # 7. Cortex AI suggestions for failed objects
            if result.deploy_errors and self.config.cortex:
                enrich_errors_with_cortex(
                    connector,
                    result.deploy_errors,
                    model=self.config.cortex_model,
                )

        result.elapsed_seconds = time.time() - t0
        return result

    # -- internals -----------------------------------------------------

    def _scan_and_parse(self) -> None:
        """Walk the objects folder, parse every .sql file."""
        root = Path(self.config.objects_folder)
        if not root.is_dir():
            log.error("Objects folder not found: %s", root)
            return

        sql_files = sorted(root.rglob("*.sql"))
        log.info("Scanning %d SQL files in '%s'", len(sql_files), root)

        self._parser.violations.clear()
        self._objects.clear()
        for path in sql_files:
            try:
                objs = self._parser.parse_file(str(path))
                for obj in objs:
                    if obj.fqn in self._objects:
                        log.warning(
                            "Duplicate object %s in %s (already defined in %s) -- last one wins",
                            obj.fqn, path, self._objects[obj.fqn].file_path,
                        )
                    self._objects[obj.fqn] = obj
            except Exception as exc:
                log.error("Failed to parse %s: %s", path, exc)

        # Check for policy violations after scanning ALL files
        if self._parser.violations:
            raise PolicyError(self._parser.violations)

    def _build_graph(self) -> None:
        """Add all parsed objects to the graph and build edges."""
        self._graph = DependencyGraph()
        for obj in self._objects.values():
            self._graph.add_object(obj)
        self._graph.build()

    def _dry_run(self, ordered: List[ObjectDefinition], result: DeployResult) -> None:
        """Print what would happen without connecting to Snowflake."""
        for i, obj in enumerate(ordered, 1):
            deps = sorted(self._graph._deps.get(obj.fqn, set()))
            deps_str = f"  (after: {', '.join(deps)})" if deps else ""
            log.info("  %3d. [%s] %s%s", i, obj.object_type, obj.fqn, deps_str)
            log.info("       file: %s  checksum: %s", obj.file_path, obj.checksum[:12])
        result.deployed = len(ordered)
        result.skipped = 0
