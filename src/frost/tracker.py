"""Change tracker -- stores deployment checksums in Snowflake.

On each run frost compares the current file checksum with the last
deployed checksum.  Only files whose checksum changed (or that have
never been deployed) are executed -- *plus* any objects that transitively
depend on a changed object.
"""

import json
import logging
from datetime import datetime, timezone
from typing import Dict, Optional, Set

from frost.connector import SnowflakeConnector

log = logging.getLogger("frost")

# Default location for the tracking table
DEFAULT_TRACKING_SCHEMA = "FROST"
DEFAULT_TRACKING_TABLE  = "DEPLOY_HISTORY"


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
        database: Optional[str] = None,
    ):
        self._conn = connector
        self._schema = tracking_schema
        self._table = tracking_table
        self._fqn = f"{tracking_schema}.{tracking_table}"
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
                executed_sql    VARIANT
            )
        """)
        # Add column if table was created before this version
        self._conn.execute_single(f"""
            ALTER TABLE {self._fqn} ADD COLUMN IF NOT EXISTS executed_sql VARIANT
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
        sql_val = json.dumps(sql) if sql else None
        self._conn.execute_params(
            f"""
            INSERT INTO {self._fqn}
                (object_fqn, object_type, file_path, checksum, status,
                 error_message, executed_sql)
            VALUES
                (%s, %s, %s, %s, %s, %s, PARSE_JSON(%s))
            """,
            (fqn, obj_type, file_path, checksum, status, error_val, sql_val),
        )
