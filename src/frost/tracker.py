"""Change tracker -- stores deployment checksums in Snowflake.

On each run frost compares the current file checksum with the last
deployed checksum.  Only files whose checksum changed (or that have
never been deployed) are executed -- *plus* any objects that transitively
depend on a changed object.

The tracker also persists the **object lineage graph** so that
dependency and lineage information is available for downstream analytics.
"""

import logging
from typing import Dict, List, Optional, Set

from frost.connector import SnowflakeConnector

log = logging.getLogger("frost")

# Default location for the tracking table
DEFAULT_TRACKING_SCHEMA = "FROST"
DEFAULT_TRACKING_TABLE  = "DEPLOY_HISTORY"
DEFAULT_LINEAGE_TABLE   = "OBJECT_LINEAGE"


class ChangeTracker:
    """Manages the deploy history table in Snowflake.

    The tracking table lives inside the target database as a schema
    (e.g. ``FROST.DEPLOY_HISTORY``).  The database is already selected
    via ``USE DATABASE`` at connection time, so we only need the
    schema-qualified name here.
    """

    def __init__(
        self,
        connector: SnowflakeConnector,
        tracking_schema: str = DEFAULT_TRACKING_SCHEMA,
        tracking_table: str = DEFAULT_TRACKING_TABLE,
        lineage_table: str = DEFAULT_LINEAGE_TABLE,
    ):
        self._conn = connector
        self._schema = tracking_schema
        self._table = tracking_table
        self._lineage_table = lineage_table
        self._fqn = f"{tracking_schema}.{tracking_table}"
        self._lineage_fqn = f"{tracking_schema}.{lineage_table}"
        self._deployed_checksums: Dict[str, str] = {}

    # -- public API ----------------------------------------------------

    def ensure_tracking_table(self) -> None:
        """Create the tracking schema and table if they don't exist."""
        log.info("Ensuring tracking table %s exists", self._fqn)
        self._conn.execute(f"CREATE SCHEMA IF NOT EXISTS {self._schema}")
        self._conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._fqn} (
                id              NUMBER AUTOINCREMENT PRIMARY KEY,
                object_fqn      VARCHAR(500)  NOT NULL,
                object_type     VARCHAR(100)  NOT NULL,
                file_path       VARCHAR(1000) NOT NULL,
                checksum        VARCHAR(64)   NOT NULL,
                status          VARCHAR(20)   NOT NULL,
                deployed_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                deployed_by     VARCHAR(200)  DEFAULT CURRENT_USER(),
                error_message   VARCHAR(5000),
                executed_sql    TEXT
            )
        """)

    def load_checksums(self) -> Dict[str, str]:
        """Load the last successful checksum for every object."""
        rows = self._conn.execute(f"""
            SELECT object_fqn, checksum
            FROM {self._fqn}
            WHERE status = 'SUCCESS'
            QUALIFY ROW_NUMBER() OVER (
                PARTITION BY object_fqn ORDER BY deployed_at DESC
            ) = 1
        """)
        self._deployed_checksums = {row[0]: row[1] for row in rows}
        return self._deployed_checksums

    def has_changed(self, fqn: str, checksum: str) -> bool:
        """True if the object has never been deployed or its checksum differs."""
        return self._deployed_checksums.get(fqn) != checksum

    def get_changed_fqns(self, current: Dict[str, str]) -> Set[str]:
        """Return FQNs whose checksum differs from the last deployment."""
        return {
            fqn for fqn, cksum in current.items()
            if self.has_changed(fqn, cksum)
        }

    def record_success(self, fqn: str, obj_type: str, file_path: str, checksum: str, sql: str = "") -> None:
        self._record(fqn, obj_type, file_path, checksum, "SUCCESS", sql=sql)

    def record_failure(self, fqn: str, obj_type: str, file_path: str, checksum: str, error: str, sql: str = "") -> None:
        self._record(fqn, obj_type, file_path, checksum, "FAILED", error=error, sql=sql)

    def record_skip(self, fqn: str, obj_type: str, file_path: str, checksum: str) -> None:
        self._record(fqn, obj_type, file_path, checksum, "SKIPPED")

    # -- lineage / graph storage ---------------------------------------

    def ensure_lineage_table(self) -> None:
        """Create the lineage table if it doesn't exist."""
        log.info("Ensuring lineage table %s exists", self._lineage_fqn)
        self._conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {self._lineage_fqn} (
                id              NUMBER AUTOINCREMENT PRIMARY KEY,
                object_fqn      VARCHAR(500)  NOT NULL,
                object_type     VARCHAR(100)  NOT NULL,
                edge_type       VARCHAR(20)   NOT NULL,
                related_fqn     VARCHAR(500)  NOT NULL,
                file_path       VARCHAR(1000),
                description     VARCHAR(5000),
                recorded_at     TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                recorded_by     VARCHAR(200)  DEFAULT CURRENT_USER()
            )
        """)

    def store_graph(self, edges: List[dict], file_paths: Optional[Dict[str, str]] = None) -> int:
        """Persist the full dependency + lineage graph.

        Replaces all existing rows with the current snapshot so the table
        always reflects the latest deploy.

        Parameters
        ----------
        edges : list of dict
            Each dict has keys ``source``, ``target``, ``type``
            (``"dependency"``, ``"reads"``, ``"writes"``).
        file_paths : dict or None
            Maps FQN -> file_path for annotation.

        Returns
        -------
        int
            Number of edges stored.
        """
        file_paths = file_paths or {}

        # Truncate and reload (snapshot approach)
        self._conn.execute(f"TRUNCATE TABLE IF EXISTS {self._lineage_fqn}")

        count = 0
        for edge in edges:
            obj_fqn = edge["source"]
            related = edge["target"]
            edge_type = edge["type"].upper()  # DEPENDENCY | READS | WRITES
            obj_type = edge.get("object_type", "UNKNOWN")
            fp = file_paths.get(obj_fqn, "")
            desc = edge.get("description", "")

            self._conn.execute_params(
                f"""
                INSERT INTO {self._lineage_fqn}
                    (object_fqn, object_type, edge_type, related_fqn,
                     file_path, description)
                VALUES
                    (%s, %s, %s, %s, %s, %s)
                """,
                (obj_fqn, obj_type, edge_type, related, fp, desc or None),
            )
            count += 1

        log.info("Stored %d graph edges in %s", count, self._lineage_fqn)
        return count

    # -- internal ------------------------------------------------------

    def _record(
        self,
        fqn: str,
        obj_type: str,
        file_path: str,
        checksum: str,
        status: str,
        error: Optional[str] = None,
        sql: str = "",
    ) -> None:
        error_val = error[:5000] if error else None
        sql_val = sql if sql else None
        self._conn.execute_params(
            f"""
            INSERT INTO {self._fqn}
                (object_fqn, object_type, file_path, checksum, status,
                 error_message, executed_sql)
            VALUES
                (%s, %s, %s, %s, %s, %s, %s)
            """,
            (fqn, obj_type, file_path, checksum, status, error_val, sql_val),
        )
