"""SQL parser -- extracts Snowflake object definitions and their dependencies.

Scans SQL files for CREATE statements to identify *what* each file defines,
then detects FROM / JOIN / REFERENCES / ON TABLE / GRANT ON patterns to
discover *what* each object depends on.

For edge cases, files can include an annotation comment:
    -- @depends_on: SCHEMA.VIEW_A, SCHEMA.TABLE_B
"""

import hashlib
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


# ----------------------------------------------------------------------
# Data model
# ----------------------------------------------------------------------

@dataclass
class ObjectDefinition:
    """A Snowflake object extracted from a SQL file."""

    file_path: str
    object_type: str            # TABLE, VIEW, PROCEDURE, ...
    database: Optional[str]
    schema: Optional[str]
    name: str
    raw_sql: str                # Original SQL (with {{variables}})
    resolved_sql: str           # SQL after variable substitution
    dependencies: Set[str]      = field(default_factory=set)
    checksum: str               = ""

    def __post_init__(self):
        if not self.checksum:
            self.checksum = hashlib.md5(self.raw_sql.encode()).hexdigest()

    @property
    def fqn(self) -> str:
        """Fully-qualified name in UPPER CASE."""
        parts = [p for p in (self.database, self.schema, self.name) if p]
        return ".".join(parts).upper()

    def __repr__(self) -> str:
        deps = ", ".join(sorted(self.dependencies)) if self.dependencies else "none"
        return f"<{self.object_type} {self.fqn} deps=[{deps}]>"


# ----------------------------------------------------------------------
# Compiled patterns (all case-insensitive)
# ----------------------------------------------------------------------

_IDENT = r"(?:\"[^\"]+\"|[\w]+)"                       # Optionally quoted identifier
_QUALIFIED = rf"(?:{_IDENT}\.)?(?:{_IDENT}\.)?{_IDENT}" # Up to 3-part name

_CREATE_RE = re.compile(
    r"CREATE\s+"
    r"(?:(OR\s+ALTER|OR\s+REPLACE)\s+)?"
    r"(?:TEMPORARY\s+|TRANSIENT\s+|VOLATILE\s+|SECURE\s+|EXTERNAL\s+|DYNAMIC\s+)*"
    r"(TABLE|(?:MATERIALIZED\s+)?VIEW|PROCEDURE|FUNCTION|STREAM|TASK|PIPE"
    r"|STAGE|FILE\s+FORMAT|SEQUENCE|DATABASE|SCHEMA)"
    r"\s+(?:IF\s+NOT\s+EXISTS\s+)?"
    rf"({_QUALIFIED})",
    re.IGNORECASE,
)

_USE_DB_RE      = re.compile(r"USE\s+DATABASE\s+([\w]+)",                             re.IGNORECASE)
_USE_SCHEMA_RE  = re.compile(r"USE\s+SCHEMA\s+((?:[\w]+\.)?[\w]+)",                   re.IGNORECASE)

_FROM_RE        = re.compile(rf"\bFROM\s+({_QUALIFIED})",                              re.IGNORECASE)
_JOIN_RE        = re.compile(rf"\bJOIN\s+({_QUALIFIED})",                              re.IGNORECASE)
_REFS_RE        = re.compile(rf"\bREFERENCES\s+({_QUALIFIED})",                        re.IGNORECASE)
_ON_TABLE_RE    = re.compile(rf"\bON\s+TABLE\s+({_QUALIFIED})",                        re.IGNORECASE)
_ON_VIEW_RE     = re.compile(rf"\bON\s+VIEW\s+({_QUALIFIED})",                         re.IGNORECASE)
_INSERT_RE      = re.compile(rf"\bINSERT\s+(?:INTO|OVERWRITE\s+INTO)\s+({_QUALIFIED})", re.IGNORECASE)
_GRANT_ON_RE    = re.compile(
    r"\bGRANT\s+.+?\bON\s+"
    r"(?:TABLE|VIEW|SCHEMA|DATABASE|PROCEDURE|FUNCTION|STREAM|TASK"
    r"|STAGE|SEQUENCE|PIPE|FILE\s+FORMAT"
    r"|ALL\s+\w+\s+IN\s+(?:SCHEMA|DATABASE))"
    rf"\s+({_QUALIFIED})",
    re.IGNORECASE,
)

_DEPENDS_ON_RE = re.compile(r"--\s*@depends_on:\s*(.+)", re.IGNORECASE)

# Names that should never be treated as object references
_KEYWORDS: Set[str] = {
    "SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "IN", "EXISTS",
    "GROUP", "BY", "ORDER", "HAVING", "LIMIT", "OFFSET", "UNION",
    "ALL", "DISTINCT", "AS", "ON", "USING", "LEFT", "RIGHT",
    "INNER", "OUTER", "FULL", "CROSS", "NATURAL", "JOIN",
    "INSERT", "INTO", "VALUES", "UPDATE", "SET", "DELETE",
    "CREATE", "ALTER", "DROP", "TRUNCATE", "REPLACE",
    "TABLE", "VIEW", "DATABASE", "SCHEMA", "PROCEDURE", "FUNCTION",
    "IF", "THEN", "ELSE", "END", "WHEN", "CASE", "BEGIN",
    "DECLARE", "RETURN", "RETURNS", "LANGUAGE", "SQL", "JAVASCRIPT",
    "PYTHON", "JAVA", "SCALA",
    "TRUE", "FALSE", "NULL",
    "INTEGER", "INT", "NUMBER", "VARCHAR", "STRING", "TEXT",
    "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
    "DATE", "TIME", "BOOLEAN", "FLOAT", "DOUBLE", "VARIANT",
    "OBJECT", "ARRAY", "BINARY", "GEOGRAPHY", "GEOMETRY",
    "AUTOINCREMENT", "IDENTITY", "PRIMARY", "KEY", "UNIQUE",
    "DEFAULT", "CONSTRAINT", "FOREIGN", "REFERENCES", "CHECK",
    "IDENTIFIER", "DUAL", "INFORMATION_SCHEMA",
    "LATERAL", "FLATTEN", "RESULT_SCAN", "LAST_QUERY_ID",
    "GENERATOR", "ROWCOUNT", "SYSTEM", "METADATA", "RAW",
    "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_USER",
    "SYSDATE", "GETDATE", "ROLE", "WAREHOUSE", "GRANT", "REVOKE",
    "USAGE", "OWNERSHIP", "FUTURE", "TABLES", "VIEWS", "SCHEMAS",
    "COPY", "PUT", "GET", "LIST", "REMOVE",
}

# Object types that support CREATE OR ALTER in Snowflake.
# These MUST use CREATE OR ALTER (not CREATE OR REPLACE).
# See: https://docs.snowflake.com/en/sql-reference/sql/create-or-alter
_CREATE_OR_ALTER_TYPES: Set[str] = {
    "TABLE", "VIEW", "MATERIALIZED VIEW", "DYNAMIC TABLE",
    "SCHEMA", "DATABASE",
    "PROCEDURE", "FUNCTION", "EXTERNAL FUNCTION",
    "TASK", "STAGE", "FILE FORMAT", "TAG",
    "ROLE", "WAREHOUSE",
}


# ----------------------------------------------------------------------
# Parser
# ----------------------------------------------------------------------

class SqlParser:
    """Parse SQL files to extract Snowflake object definitions and dependencies."""

    def __init__(self, variables: Optional[Dict[str, str]] = None):
        self.variables = variables or {}

    # -- public API ----------------------------------------------------

    def parse_file(self, file_path: str) -> List[ObjectDefinition]:
        """Return every object defined in *file_path* with its dependencies."""
        path = Path(file_path)
        raw_sql = path.read_text(encoding="utf-8")
        resolved_sql = self._substitute_variables(raw_sql)
        clean = self._strip_comments_and_strings(resolved_sql)

        # Context from USE statements
        default_db, default_schema = self._extract_use_context(clean)

        # Explicit dependency annotations (-- @depends_on: ...)
        explicit_deps = self._extract_explicit_deps(resolved_sql)

        # Object definitions
        objects = self._extract_objects(
            clean, raw_sql, resolved_sql, str(path), default_db, default_schema,
        )

        # Dependency references
        auto_deps = self._extract_references(clean, default_db, default_schema)

        # Merge dependencies into each object (skip self-references)
        for obj in objects:
            all_deps = (auto_deps | explicit_deps) - {obj.fqn}
            obj.dependencies = all_deps

        # If no CREATE was found, represent the file as an opaque script
        if not objects:
            obj = ObjectDefinition(
                file_path=str(path),
                object_type="SCRIPT",
                database=default_db,
                schema=default_schema,
                name=path.stem.upper(),
                raw_sql=raw_sql,
                resolved_sql=resolved_sql,
                dependencies=(auto_deps | explicit_deps),
            )
            objects.append(obj)

        return objects

    # -- internal helpers ----------------------------------------------

    def _substitute_variables(self, sql: str) -> str:
        for key, value in self.variables.items():
            sql = sql.replace("{{" + key + "}}", str(value))
        return sql

    @staticmethod
    def _strip_comments_and_strings(sql: str) -> str:
        """Remove comments and string literals to avoid false-positive matches."""
        # Block comments
        sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
        # Line comments (keep @depends_on lines -- they're parsed separately)
        sql = re.sub(r"--(?!\s*@depends_on).*$", " ", sql, flags=re.MULTILINE)
        # Single-quoted strings
        sql = re.sub(r"'[^']*'", "''", sql)
        # Double-quoted identifiers are kept (they are object names)
        return sql

    @staticmethod
    def _extract_use_context(sql: str) -> Tuple[Optional[str], Optional[str]]:
        db = schema = None
        for m in _USE_DB_RE.finditer(sql):
            db = m.group(1).upper()
        for m in _USE_SCHEMA_RE.finditer(sql):
            ref = m.group(1).upper()
            if "." in ref:
                db, schema = ref.split(".", 1)
            else:
                schema = ref
        return db, schema

    @staticmethod
    def _extract_explicit_deps(sql: str) -> Set[str]:
        deps: Set[str] = set()
        for m in _DEPENDS_ON_RE.finditer(sql):
            for ref in m.group(1).split(","):
                ref = ref.strip().upper()
                if ref:
                    deps.add(ref)
        return deps

    def _extract_objects(
        self,
        clean_sql: str,
        raw_sql: str,
        resolved_sql: str,
        file_path: str,
        default_db: Optional[str],
        default_schema: Optional[str],
    ) -> List[ObjectDefinition]:
        objects: List[ObjectDefinition] = []
        for m in _CREATE_RE.finditer(clean_sql):
            modifier = (m.group(1) or "").upper()  # 'OR ALTER', 'OR REPLACE', or ''
            obj_type = re.sub(r"\s+", " ", m.group(2).upper())
            db, schema, name = self._resolve_name(m.group(3), default_db, default_schema)

            # Enforce CREATE OR ALTER for types that support it
            is_or_alter = "ALTER" in modifier
            is_or_replace = "REPLACE" in modifier
            if obj_type in _CREATE_OR_ALTER_TYPES and not is_or_alter:
                fqn = ".".join(p for p in (db, schema, name) if p).upper()
                raise SyntaxError(
                    f"{file_path}: {obj_type} {fqn} must use CREATE OR ALTER "
                    f"(Snowflake supports it for this type). "
                    f"Found: {'CREATE OR REPLACE' if is_or_replace else 'CREATE'}. "
                    f"See: https://docs.snowflake.com/en/sql-reference/sql/create-or-alter"
                )

            objects.append(ObjectDefinition(
                file_path=file_path,
                object_type=obj_type,
                database=db,
                schema=schema,
                name=name,
                raw_sql=raw_sql,
                resolved_sql=resolved_sql,
            ))
        return objects

    def _extract_references(
        self, sql: str, default_db: Optional[str], default_schema: Optional[str],
    ) -> Set[str]:
        refs: Set[str] = set()
        for pattern in (_FROM_RE, _JOIN_RE, _REFS_RE, _ON_TABLE_RE,
                        _ON_VIEW_RE, _INSERT_RE, _GRANT_ON_RE):
            for m in pattern.finditer(sql):
                raw = m.group(1).strip()
                if raw.upper() in _KEYWORDS:
                    continue
                db, schema, name = self._resolve_name(raw, default_db, default_schema)
                fqn = ".".join(p for p in (db, schema, name) if p).upper()
                if fqn:
                    refs.add(fqn)
        return refs

    @staticmethod
    def _resolve_name(
        raw: str, default_db: Optional[str], default_schema: Optional[str],
    ) -> Tuple[Optional[str], Optional[str], str]:
        """Split a potentially qualified name into (database, schema, name).

        Convention: SQL files use SCHEMA.OBJECT names.  The database is
        set at the connection level, so we never inject a default database.
        """
        # Strip double quotes from each part
        parts = [p.strip('"').upper() for p in raw.split(".")]
        if len(parts) == 3:
            # Explicit 3-part name -- keep as-is (user override)
            return parts[0], parts[1], parts[2]
        if len(parts) == 2:
            return None, parts[0], parts[1]
        return None, default_schema, parts[0]
