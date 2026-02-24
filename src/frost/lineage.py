"""Procedure lineage -- declare source / target relationships for documentation.

Snowflake does not validate object references inside procedure bodies at
CREATE time.  Frost therefore does **not** enforce lineage during deploy,
but it lets you document the data flow in YAML sidecars that live next to
your SQL files.

Example sidecar (``objects/procedures/refresh_summary.yml``)::

    sources:
      - PUBLIC.ORDERS
      - PUBLIC.CUSTOMERS
    targets:
      - PUBLIC.CUSTOMER_SUMMARY
    description: >
      Aggregates order data per customer into the summary table.

Lineage entries are:
* Shown by ``frost graph`` alongside auto-parsed dependencies.
* Stored in the ``FROST.OBJECT_LINEAGE`` table on every deploy.
"""

import logging
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Set

import yaml

log = logging.getLogger("frost")


# ------------------------------------------------------------------
# Data model
# ------------------------------------------------------------------

@dataclass
class LineageEntry:
    """A declared source/target relationship for a SQL object."""

    object_fqn: str              # The procedure / function that declares this lineage
    file_path: str               # Path to the SQL file
    sources: List[str] = field(default_factory=list)   # Objects read by this proc
    targets: List[str] = field(default_factory=list)   # Objects written by this proc
    description: str = ""


# ------------------------------------------------------------------
# Lineage scanner
# ------------------------------------------------------------------

class LineageScanner:
    """Discover YAML lineage sidecars next to SQL files in the objects folder.

    A sidecar is a ``.yml`` or ``.yaml`` file that shares the same stem
    as a ``.sql`` file::

        objects/procedures/
            refresh_summary.sql
            refresh_summary.yml   <-- lineage sidecar
    """

    def __init__(self, objects_folder: str):
        self.objects_folder = Path(objects_folder)

    def scan(self) -> List[LineageEntry]:
        """Walk the objects folder and return all lineage entries."""
        if not self.objects_folder.is_dir():
            log.warning("Objects folder not found: %s", self.objects_folder)
            return []

        entries: List[LineageEntry] = []
        for yml_path in sorted(self.objects_folder.rglob("*.yml")):
            entry = self._parse_sidecar(yml_path)
            if entry:
                entries.append(entry)
        for yml_path in sorted(self.objects_folder.rglob("*.yaml")):
            entry = self._parse_sidecar(yml_path)
            if entry:
                entries.append(entry)
        return entries

    def _parse_sidecar(self, yml_path: Path) -> Optional[LineageEntry]:
        """Parse a single YAML sidecar and return a LineageEntry (or None)."""
        # Corresponding SQL file must exist
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

        # Normalise names to upper case
        sources = [self._normalise_fqn(s) for s in sources_raw]
        targets = [self._normalise_fqn(t) for t in targets_raw]

        # Derive the object FQN from the SQL file
        # We use the file's relative path to infer the object name
        # The actual FQN is resolved later when merging with the parsed graph
        object_fqn = self._fqn_from_path(sql_path)

        return LineageEntry(
            object_fqn=object_fqn,
            file_path=str(sql_path),
            sources=sources,
            targets=targets,
            description=raw.get("description", "").strip(),
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
