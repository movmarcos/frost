"""Procedure lineage -- auto-detect source / target relationships.

frost analyses procedure and function bodies to **automatically discover**
which objects they read from and write to.  This gives a complete
data-flow map with zero configuration.

Auto-detection works by extracting the SQL body (inside ``$$...$$``, a
single-quoted string, or a ``BEGIN…END`` block) and scanning for
DML / query patterns:

* **Sources** (reads): ``FROM``, ``JOIN``
* **Targets** (writes): ``INSERT INTO``, ``UPDATE … SET``,
  ``DELETE FROM``, ``MERGE INTO``, ``COPY INTO``,
  ``CREATE TABLE … AS``, ``TRUNCATE``

For edge cases (dynamic SQL, ``EXECUTE IMMEDIATE``, non-SQL language
bodies that embed SQL as strings) a YAML sidecar can be placed next to
the SQL file to **override** auto-detected lineage.

Lineage entries are:

* Shown by ``frost graph`` alongside auto-parsed dependencies.
* Stored in the ``FROST.OBJECT_LINEAGE`` table on every deploy.
"""

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Set

import yaml

if TYPE_CHECKING:
    from frost.parser import ObjectDefinition

log = logging.getLogger("frost")


# ------------------------------------------------------------------
# Identifier patterns (same convention as parser.py)
# ------------------------------------------------------------------

_IDENT = r"(?:\"[^\"]+\"|[\w]+)"
_QUALIFIED = rf"(?:{_IDENT}\.)?(?:{_IDENT}\.)?{_IDENT}"

# ------------------------------------------------------------------
# Body extraction
# ------------------------------------------------------------------

_DOLLAR_BODY_RE = re.compile(
    r"\$([A-Za-z_]?\w*)\$(.*?)\$\1\$", re.DOTALL | re.IGNORECASE,
)
_STRING_BODY_RE = re.compile(
    r"\bAS\s+'((?:[^']|'')*)'", re.DOTALL | re.IGNORECASE,
)
_BEGIN_BODY_RE = re.compile(
    r"\bAS\s*\n?\s*(BEGIN\b.*?\bEND\s*;)", re.DOTALL | re.IGNORECASE,
)

# ------------------------------------------------------------------
# Read patterns (sources)
# ------------------------------------------------------------------

_FROM_RE = re.compile(rf"\bFROM\s+({_QUALIFIED})", re.IGNORECASE)
_JOIN_RE = re.compile(rf"\bJOIN\s+({_QUALIFIED})", re.IGNORECASE)

# ------------------------------------------------------------------
# Write patterns (targets)
# ------------------------------------------------------------------

_INSERT_RE = re.compile(
    rf"\bINSERT\s+(?:INTO|OVERWRITE\s+INTO)\s+({_QUALIFIED})", re.IGNORECASE,
)
_UPDATE_RE = re.compile(
    rf"\bUPDATE\s+({_QUALIFIED})\s+SET\b", re.IGNORECASE,
)
_DELETE_RE = re.compile(
    rf"\bDELETE\s+FROM\s+({_QUALIFIED})", re.IGNORECASE,
)
_MERGE_RE = re.compile(
    rf"\bMERGE\s+INTO\s+({_QUALIFIED})", re.IGNORECASE,
)
_COPY_INTO_RE = re.compile(
    rf"\bCOPY\s+INTO\s+({_QUALIFIED})", re.IGNORECASE,
)
_CTAS_RE = re.compile(
    rf"\bCREATE\s+(?:OR\s+\w+\s+)?(?:TEMPORARY\s+|TEMP\s+)?TABLE\s+"
    rf"({_QUALIFIED})\s+AS\b",
    re.IGNORECASE,
)
_TRUNCATE_RE = re.compile(
    rf"\bTRUNCATE\s+(?:TABLE\s+)?(?:IF\s+EXISTS\s+)?({_QUALIFIED})",
    re.IGNORECASE,
)

_SOURCE_PATTERNS = [_FROM_RE, _JOIN_RE]
_TARGET_PATTERNS = [
    _INSERT_RE, _UPDATE_RE, _DELETE_RE, _MERGE_RE,
    _COPY_INTO_RE, _CTAS_RE, _TRUNCATE_RE,
]

# ------------------------------------------------------------------
# Dynamic SQL markers — when present, auto-detection is unreliable
# ------------------------------------------------------------------

_DYNAMIC_SQL_PATTERNS: list = [
    re.compile(r"\bEXECUTE\s+IMMEDIATE\b", re.IGNORECASE),
    re.compile(r"\bIDENTIFIER\s*\(", re.IGNORECASE),
    re.compile(r"\bSYSTEM\$QUERY_REFERENCE\b", re.IGNORECASE),
    re.compile(r"\bRESULTSET\b", re.IGNORECASE),
    # String concatenation used to build SQL: 'SELECT * FROM ' || var
    re.compile(
        r"'[^']*(?:FROM|JOIN|INSERT|UPDATE|DELETE|MERGE|INTO)\s+[^']*'\s*\|\|",
        re.IGNORECASE,
    ),
]

# ------------------------------------------------------------------
# Keywords / noise to exclude from matches
# ------------------------------------------------------------------

_NOISE: Set[str] = {
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
    "TRUE", "FALSE", "NULL", "LET", "FOR", "DO", "WHILE",
    "RESULTSET", "CURSOR", "OPEN", "FETCH", "CLOSE",
    "DUAL", "INFORMATION_SCHEMA", "LATERAL", "FLATTEN",
    "RESULT_SCAN", "LAST_QUERY_ID", "GENERATOR", "SYSTEM",
    "IDENTIFIER", "TABLE_STREAM", "METADATA",
    "INTEGER", "INT", "NUMBER", "VARCHAR", "STRING", "TEXT",
    "TIMESTAMP", "TIMESTAMP_NTZ", "TIMESTAMP_LTZ", "TIMESTAMP_TZ",
    "DATE", "TIME", "BOOLEAN", "FLOAT", "DOUBLE", "VARIANT",
    "OBJECT", "ARRAY", "BINARY",
}


# ------------------------------------------------------------------
# Data model
# ------------------------------------------------------------------

@dataclass
class LineageEntry:
    """A source/target relationship set for a SQL object."""

    object_fqn: str              # The procedure / function FQN
    file_path: str               # Path to the SQL file
    sources: List[str] = field(default_factory=list)
    targets: List[str] = field(default_factory=list)
    description: str = ""
    auto_detected: bool = False  # True when inferred from SQL body


# ------------------------------------------------------------------
# Procedure body analyser
# ------------------------------------------------------------------

class ProcedureBodyAnalyzer:
    """Extract read/write object references from procedure and function bodies.

    Snowflake procedures embed their logic inside ``$$…$$`` blocks,
    single-quoted strings, or ``BEGIN…END`` blocks.  This analyser
    extracts the body and scans it for DML/query patterns.
    """

    def analyze(self, obj: "ObjectDefinition") -> Optional[LineageEntry]:
        """Analyse a parsed ``ObjectDefinition`` and return a ``LineageEntry``.

        Returns ``None`` if the object is not a PROCEDURE / FUNCTION,
        if no read/write references are found, or if the body contains
        dynamic SQL (which makes pattern-based detection unreliable).
        """
        if obj.object_type not in ("PROCEDURE", "FUNCTION"):
            return None

        body = self._extract_body(obj.raw_sql)
        if not body:
            return None

        body = self._strip_body_comments(body)

        if self._has_dynamic_sql(body):
            log.info(
                "Skipping auto-detection for %s — dynamic SQL detected. "
                "Use a YAML sidecar to declare lineage manually.",
                obj.fqn,
            )
            return None

        sources = self._find_references(body, _SOURCE_PATTERNS)
        targets = self._find_references(body, _TARGET_PATTERNS)

        # Remove the procedure's own FQN from sources / targets
        own = obj.fqn
        sources = [s for s in sources if s != own]
        targets = [t for t in targets if t != own]

        if not sources and not targets:
            return None

        return LineageEntry(
            object_fqn=obj.fqn,
            file_path=obj.file_path,
            sources=sorted(set(sources)),
            targets=sorted(set(targets)),
            auto_detected=True,
        )

    # -- body extraction -----------------------------------------------

    @staticmethod
    def _extract_body(sql: str) -> Optional[str]:
        """Pull the procedure/function body from the CREATE statement.

        Supports dollar-quoted (``$$…$$``), ``BEGIN…END`` (Snowflake
        Scripting), and single-quoted (``AS '…'``) bodies.
        """
        m = _DOLLAR_BODY_RE.search(sql)
        if m:
            return m.group(2)

        m = _BEGIN_BODY_RE.search(sql)
        if m:
            return m.group(1)

        m = _STRING_BODY_RE.search(sql)
        if m:
            return m.group(1).replace("''", "'")

        return None

    @staticmethod
    def _strip_body_comments(body: str) -> str:
        """Remove block and line comments from the body text."""
        body = re.sub(r"/\*.*?\*/", " ", body, flags=re.DOTALL)
        body = re.sub(r"--.*$", " ", body, flags=re.MULTILINE)
        return body

    @staticmethod
    def _has_dynamic_sql(body: str) -> bool:
        """Return ``True`` if the body contains dynamic SQL markers.

        When dynamic SQL is present (e.g. ``EXECUTE IMMEDIATE``,
        ``IDENTIFIER()``, string-concatenated queries), regex-based
        pattern matching produces unreliable results so frost skips
        auto-detection and relies on a YAML sidecar instead.
        """
        return any(pat.search(body) for pat in _DYNAMIC_SQL_PATTERNS)

    @staticmethod
    def _find_references(body: str, patterns: list) -> List[str]:
        """Match all qualified names from the given patterns, excluding noise."""
        refs: List[str] = []
        for pat in patterns:
            for m in pat.finditer(body):
                raw = m.group(1).strip()
                parts = [p.strip('"').upper() for p in raw.split(".")]
                # Only filter single-part names that are SQL keywords;
                # qualified names like PUBLIC.IN are real object refs.
                if len(parts) == 1 and parts[0] in _NOISE:
                    continue
                fqn = ".".join(p for p in parts if p)
                if fqn:
                    refs.append(fqn)
        return refs


# ------------------------------------------------------------------
# Lineage scanner
# ------------------------------------------------------------------

class LineageScanner:
    """Discover lineage by analysing procedure SQL bodies and YAML sidecars.

    **Auto-detection** is the primary method — no configuration needed.
    frost parses procedure / function bodies and identifies FROM, JOIN,
    INSERT, UPDATE, DELETE, MERGE, COPY, TRUNCATE, and CTAS patterns.

    **YAML sidecars** provide overrides for edge cases (dynamic SQL,
    non-SQL language bodies, etc.).  When a YAML declares ``sources``
    or ``targets`` those values **replace** the auto-detected ones for
    that field, giving the user full control.
    """

    def __init__(self, objects_folder: str):
        self.objects_folder = Path(objects_folder)
        self._analyzer = ProcedureBodyAnalyzer()

    def scan(
        self,
        parsed_objects: Optional[Dict[str, "ObjectDefinition"]] = None,
    ) -> List[LineageEntry]:
        """Auto-detect lineage from procedure bodies and merge with YAML overrides.

        Parameters
        ----------
        parsed_objects : dict or None
            Maps FQN → ``ObjectDefinition``.  When provided, every
            PROCEDURE or FUNCTION is analysed automatically.
        """
        entries: Dict[str, LineageEntry] = {}  # keyed by file_path

        # 1. Auto-detect from procedure / function SQL bodies
        if parsed_objects:
            for _fqn, obj in sorted(parsed_objects.items()):
                entry = self._analyzer.analyze(obj)
                if entry:
                    entries[entry.file_path] = entry

        # 2. Scan for YAML sidecars (override / supplement)
        yaml_entries = self._scan_yaml_sidecars()
        for ye in yaml_entries:
            existing = entries.get(ye.file_path)
            if existing:
                # YAML overrides auto-detected for every field it declares
                if ye.sources:
                    existing.sources = ye.sources
                if ye.targets:
                    existing.targets = ye.targets
                if ye.description:
                    existing.description = ye.description
                existing.auto_detected = False  # now manually curated
            else:
                entries[ye.file_path] = ye

        return list(entries.values())

    # -- YAML sidecar scanning -----------------------------------------

    def _scan_yaml_sidecars(self) -> List[LineageEntry]:
        """Walk the objects folder for YAML sidecars."""
        if not self.objects_folder.is_dir():
            return []

        results: List[LineageEntry] = []
        for pattern in ("*.yml", "*.yaml"):
            for yml_path in sorted(self.objects_folder.rglob(pattern)):
                entry = self._parse_sidecar(yml_path)
                if entry:
                    results.append(entry)
        return results

    def _parse_sidecar(self, yml_path: Path) -> Optional[LineageEntry]:
        """Parse a single YAML sidecar and return a LineageEntry (or None)."""
        sql_path = yml_path.with_suffix(".sql")
        if not sql_path.exists():
            log.debug("No matching SQL file for %s -- skipping", yml_path)
            return None

        raw = yaml.safe_load(yml_path.read_text(encoding="utf-8"))
        if not raw:
            return None

        sources_raw = raw.get("sources", [])
        targets_raw = raw.get("targets", [])

        if not sources_raw and not targets_raw:
            return None  # No lineage declared

        sources = [self._normalise_fqn(s) for s in sources_raw]
        targets = [self._normalise_fqn(t) for t in targets_raw]
        object_fqn = self._fqn_from_path(sql_path)

        return LineageEntry(
            object_fqn=object_fqn,
            file_path=str(sql_path),
            sources=sources,
            targets=targets,
            description=raw.get("description", "").strip(),
            auto_detected=False,
        )

    @staticmethod
    def _normalise_fqn(name: str) -> str:
        """Normalise an FQN to upper case, stripping whitespace."""
        return name.strip().upper()

    def _fqn_from_path(self, sql_path: Path) -> str:
        """Derive a candidate FQN from the SQL file path.

        This is a *best effort* — the real FQN comes from the parser.
        We use ``STEM.upper()`` so it can be matched against the parsed
        graph later.
        """
        return sql_path.stem.upper()


def merge_lineage_with_graph(
    lineage_entries: List[LineageEntry],
    parsed_fqns: Dict[str, str],
) -> List[LineageEntry]:
    """Resolve lineage entry FQNs against the parsed object graph.

    ``parsed_fqns`` maps ``file_path -> actual_fqn`` from the parser.
    If a lineage entry's SQL file was parsed, we update its
    ``object_fqn`` to the real FQN.
    """
    resolved: List[LineageEntry] = []
    for entry in lineage_entries:
        real_fqn = parsed_fqns.get(entry.file_path)
        if real_fqn:
            entry.object_fqn = real_fqn
        resolved.append(entry)
    return resolved
