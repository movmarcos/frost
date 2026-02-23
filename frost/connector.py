"""Snowflake connection manager with RSA key-pair authentication."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, List, Optional

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import snowflake.connector

log = logging.getLogger("frost")


@dataclass
class ConnectionConfig:
    """All parameters needed to connect to Snowflake."""

    account: str
    user: str
    role: str
    warehouse: str
    database: Optional[str] = None
    private_key_path: str = ""
    private_key_passphrase: Optional[str] = None


class SnowflakeConnector:
    """Thin wrapper around the Snowflake Python connector."""

    def __init__(self, config: ConnectionConfig):
        self._config = config
        self._conn: Optional[snowflake.connector.SnowflakeConnection] = None

    # ── lifecycle ─────────────────────────────────────────────────────

    def connect(self) -> "SnowflakeConnector":
        private_key_bytes = self._load_private_key()
        params: dict[str, Any] = dict(
            account=self._config.account,
            user=self._config.user,
            private_key=private_key_bytes,
            role=self._config.role,
            warehouse=self._config.warehouse,
        )
        if self._config.database:
            params["database"] = self._config.database

        log.info(
            "Connecting to Snowflake  account=%s  user=%s  role=%s",
            self._config.account,
            self._config.user,
            self._config.role,
        )
        self._conn = snowflake.connector.connect(**params)
        return self

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def __enter__(self) -> "SnowflakeConnector":
        self.connect()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ── execution ─────────────────────────────────────────────────────

    def execute(self, sql: str) -> List[tuple]:
        """Execute one or more semicolon-separated statements.

        Returns the result of the *last* statement.
        """
        assert self._conn, "Not connected — call connect() first"
        cursor = self._conn.cursor()
        results: List[tuple] = []
        try:
            for stmt in self._split_statements(sql):
                stmt = stmt.strip()
                if not stmt:
                    continue
                log.debug("SQL ▸ %s", stmt[:200])
                cursor.execute(stmt)
                try:
                    results = cursor.fetchall()
                except snowflake.connector.ProgrammingError:
                    results = []
        finally:
            cursor.close()
        return results

    def execute_single(self, sql: str) -> List[tuple]:
        """Execute a single statement (no splitting)."""
        assert self._conn, "Not connected — call connect() first"
        cursor = self._conn.cursor()
        try:
            cursor.execute(sql.strip())
            try:
                return cursor.fetchall()
            except snowflake.connector.ProgrammingError:
                return []
        finally:
            cursor.close()

    # ── helpers ───────────────────────────────────────────────────────

    def _load_private_key(self) -> bytes:
        key_path = Path(self._config.private_key_path)
        if not key_path.is_file():
            raise FileNotFoundError(f"Private key not found: {key_path}")

        passphrase = (
            self._config.private_key_passphrase.encode()
            if self._config.private_key_passphrase
            else None
        )
        with open(key_path, "rb") as fh:
            private_key = serialization.load_pem_private_key(
                fh.read(), password=passphrase, backend=default_backend(),
            )
        return private_key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        )

    @staticmethod
    def _split_statements(sql: str) -> List[str]:
        """Naively split on semicolons outside of string literals."""
        stmts: List[str] = []
        current: List[str] = []
        in_string = False

        for char in sql:
            if char == "'" and not in_string:
                in_string = True
                current.append(char)
            elif char == "'" and in_string:
                in_string = False
                current.append(char)
            elif char == ";" and not in_string:
                stmts.append("".join(current))
                current = []
            else:
                current.append(char)

        remaining = "".join(current).strip()
        if remaining:
            stmts.append(remaining)

        return stmts
