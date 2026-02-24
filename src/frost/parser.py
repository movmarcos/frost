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

from frost.reporter import Violation


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
    columns: List[str]          = field(default_factory=list)
    checksum: str               = ""
    is_drop: bool               = False

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

_DROP_RE = re.compile(
    r"DROP\s+"
    r"(TABLE|(?:MATERIALIZED\s+)?VIEW|PROCEDURE|FUNCTION|STREAM|TASK|PIPE"
    r"|STAGE|FILE\s+FORMAT|SEQUENCE|DATABASE|SCHEMA)"
    r"\s+(?:IF\s+EXISTS\s+)?"
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

# Object types that MUST use CREATE OR ALTER (not CREATE OR REPLACE).
# Full list from Snowflake docs:
# https://docs.snowflake.com/en/sql-reference/sql/create-or-alter
_CREATE_OR_ALTER_TYPES: Set[str] = {
    # Account objects
    "DATABASE", "ROLE", "WAREHOUSE",
    # Database objects
    "DYNAMIC TABLE", "EXTERNAL FUNCTION", "FILE FORMAT",
    "FUNCTION", "PROCEDURE", "SCHEMA", "STAGE",
    "TABLE", "TASK", "VIEW", "TAG",
}


# ----------------------------------------------------------------------
# Parser
# ----------------------------------------------------------------------

class SqlParser:
    """Parse SQL files to extract Snowflake object definitions and dependencies."""

    def __init__(self, variables: Optional[Dict[str, str]] = None):
        self.variables = variables or {}
        self.violations: List[Violation] = []

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

        # DROP statements (lifecycle tracking)
        drop_objects = self._extract_drops(
            clean, raw_sql, resolved_sql, str(path), default_db, default_schema,
        )

        # Dependency references
        auto_deps = self._extract_references(clean, default_db, default_schema)

        # Merge dependencies into each object (skip self-references)
        for obj in objects:
            all_deps = (auto_deps | explicit_deps) - {obj.fqn}
            obj.dependencies = all_deps

        # If no CREATE was found, represent the file as an opaque script
        if not objects and not drop_objects:
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

        # Include DROP statements alongside CREATE objects so the
        # deployer can track lifecycle retirements.
        objects.extend(drop_objects)

        return objects

    # -- internal helpers ----------------------------------------------

    def _substitute_variables(self, sql: str) -> str:
        for key, value in self.variables.items():
            sql = sql.replace("{{" + key + "}}", str(value))
        return sql

    @staticmethod
    def _strip_comments_and_strings(sql: str) -> str:
        """Remove comments, string literals, and dollar-quoted blocks.

        This prevents the CREATE/dependency regexes from matching code
        that lives *inside* a procedure or function body.
        """
        # Block comments
        sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
        # Line comments (keep @depends_on lines -- they're parsed separately)
        sql = re.sub(r"--(?!\s*@depends_on).*$", " ", sql, flags=re.MULTILINE)
        # Dollar-quoted blocks  ($$...$$ or $tag$...$tag$)
        sql = re.sub(r"\$([A-Za-z_]?\w*)\$.*?\$\1\$", "''", sql, flags=re.DOTALL)
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

    @staticmethod
    def _find_source_line(raw_sql: str, obj_type: str, found_form: str) -> Tuple[str, int]:
        """Find the source line and 1-based line number of a CREATE statement."""
        for i, line in enumerate(raw_sql.splitlines(), 1):
            upper = line.upper().strip()
            if found_form.upper() in upper and obj_type in upper:
                return line, i
        # Fallback: first non-comment, non-blank line
        for i, line in enumerate(raw_sql.splitlines(), 1):
            stripped = line.strip()
            if stripped and not stripped.startswith("--"):
                return line, i
        return raw_sql.splitlines()[0] if raw_sql else "", 1

    @staticmethod
    def _extract_columns(clean_sql: str, obj_type: str) -> List[str]:
        """Extract column names from a CREATE TABLE statement.

        Only TABLE (and DYNAMIC TABLE) definitions have an inline column
        list.  For VIEWs, PROCEDUREs, etc. the column list is either
        derived from a query or not applicable, so we return [].
        """
        if obj_type not in ("TABLE", "DYNAMIC TABLE"):
            return []

        # Find the first parenthesised block after CREATE ... TABLE name
        m = re.search(
            r"CREATE\s+(?:OR\s+(?:ALTER|REPLACE)\s+)?"
            r"(?:TEMPORARY\s+|TRANSIENT\s+|VOLATILE\s+|EXTERNAL\s+|DYNAMIC\s+)*"
            r"TABLE\s+(?:IF\s+NOT\s+EXISTS\s+)?"
            rf"(?:{_QUALIFIED})\s*\(",
            clean_sql,
            re.IGNORECASE,
        )
        if not m:
            return []

        # Walk from opening paren to the matching close
        start = m.end() - 1  # index of the '('
        depth = 0
        body_start = start + 1
        body_end = len(clean_sql)
        for i in range(start, len(clean_sql)):
            if clean_sql[i] == "(":
                depth += 1
            elif clean_sql[i] == ")":
                depth -= 1
                if depth == 0:
                    body_end = i
                    break

        body = clean_sql[body_start:body_end]

        # Split on commas at depth-0 (skip nested parens like NUMBER(10,2))
        parts: List[str] = []
        current: List[str] = []
        depth = 0
        for ch in body:
            if ch == "(":
                depth += 1
                current.append(ch)
            elif ch == ")":
                depth -= 1
                current.append(ch)
            elif ch == "," and depth == 0:
                parts.append("".join(current).strip())
                current = []
            else:
                current.append(ch)
        if current:
            parts.append("".join(current).strip())

        # Each part that starts with an identifier (not a constraint keyword)
        # is a column definition.  Extract the first token as the column name.
        constraint_kw = {
            "PRIMARY", "FOREIGN", "UNIQUE", "CHECK", "CONSTRAINT",
            "CLUSTER", "LIKE", "AS",
        }
        columns: List[str] = []
        for part in parts:
            if not part:
                continue
            first_token = re.split(r"\s+", part, maxsplit=1)[0].upper().strip('"')
            if first_token in constraint_kw:
                continue
            columns.append(first_token)

        return columns

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
            # Procedures and functions are exempt – CREATE OR REPLACE is
            # the standard Snowflake pattern for objects with code bodies.
            _REPLACE_ALLOWED_TYPES: Set[str] = {"PROCEDURE", "FUNCTION", "EXTERNAL FUNCTION"}
            is_or_alter = "ALTER" in modifier
            is_or_replace = "REPLACE" in modifier
            if obj_type in _CREATE_OR_ALTER_TYPES and not is_or_alter and obj_type not in _REPLACE_ALLOWED_TYPES:
                found = "CREATE OR REPLACE" if is_or_replace else "CREATE"
                fqn = ".".join(p for p in (db, schema, name) if p).upper()
                source_line, line_no = self._find_source_line(raw_sql, obj_type, found)
                self.violations.append(Violation(
                    file_path=file_path,
                    object_type=obj_type,
                    fqn=fqn,
                    found_form=found,
                    suggested_form="CREATE OR ALTER",
                    source_line=source_line,
                    line_number=line_no,
                ))
                # Still append the object so we can report all violations

            objects.append(ObjectDefinition(
                file_path=file_path,
                object_type=obj_type,
                database=db,
                schema=schema,
                name=name,
                raw_sql=raw_sql,
                resolved_sql=resolved_sql,
                columns=self._extract_columns(clean_sql, obj_type),
            ))
        return objects

    def _extract_drops(
        self,
        clean_sql: str,
        raw_sql: str,
        resolved_sql: str,
        file_path: str,
        default_db: Optional[str],
        default_schema: Optional[str],
    ) -> List[ObjectDefinition]:
        """Extract DROP statements and return them as ObjectDefinitions
        with ``is_drop=True`` so the deployer can retire them from the
        lifecycle table after execution."""
        drops: List[ObjectDefinition] = []
        for m in _DROP_RE.finditer(clean_sql):
            obj_type = re.sub(r"\s+", " ", m.group(1).upper())
            db, schema, name = self._resolve_name(m.group(2), default_db, default_schema)
            drops.append(ObjectDefinition(
                file_path=file_path,
                object_type=obj_type,
                database=db,
                schema=schema,
                name=name,
                raw_sql=raw_sql,
                resolved_sql=resolved_sql,
                is_drop=True,
            ))
        return drops

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
