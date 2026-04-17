"""List live Snowflake resources for the VS Code Resource Explorer."""

import logging
from typing import Any, Dict, List

log = logging.getLogger("frost")

# Map frost-relevant resource type → SHOW command template.
# {schema} is replaced with the fully-qualified schema name.
RESOURCE_QUERIES: Dict[str, str] = {
    "TABLE":             "SHOW TABLES IN SCHEMA {schema}",
    "VIEW":              "SHOW VIEWS IN SCHEMA {schema}",
    "PROCEDURE":         "SHOW PROCEDURES IN SCHEMA {schema}",
    "FUNCTION":          "SHOW USER FUNCTIONS IN SCHEMA {schema}",
    "DYNAMIC TABLE":     "SHOW DYNAMIC TABLES IN SCHEMA {schema}",
    "FILE FORMAT":       "SHOW FILE FORMATS IN SCHEMA {schema}",
    "STAGE":             "SHOW STAGES IN SCHEMA {schema}",
    "TASK":              "SHOW TASKS IN SCHEMA {schema}",
    "TAG":               "SHOW TAGS IN SCHEMA {schema}",
    "STREAM":            "SHOW STREAMS IN SCHEMA {schema}",
    "PIPE":              "SHOW PIPES IN SCHEMA {schema}",
    "MATERIALIZED VIEW": "SHOW MATERIALIZED VIEWS IN SCHEMA {schema}",
    "ALERT":             "SHOW ALERTS IN SCHEMA {schema}",
    "EVENT TABLE":       "SHOW EVENT TABLES IN SCHEMA {schema}",
    "SEQUENCE":          "SHOW SEQUENCES IN SCHEMA {schema}",
}

_SKIP_SCHEMAS = {"INFORMATION_SCHEMA"}


def fetch_resources(connector: Any, database: str) -> Dict[str, Any]:
    """Query Snowflake for all schema-scoped resources in *database*.

    Returns a dict matching the ``frost resources --json`` output schema:
    ``{"database": ..., "resources": [...], "warnings": [...]}``.

    Individual SHOW queries that fail (e.g. insufficient privileges) are
    logged and added to ``warnings`` — they do not abort the operation.
    """
    resources: List[Dict[str, Any]] = []
    warnings: List[str] = []

    # Discover schemas
    try:
        schema_rows = connector.execute_single(
            f"SHOW SCHEMAS IN DATABASE {database}"
        )
    except Exception as exc:
        log.error("Could not list schemas in %s: %s", database, exc)
        return {"database": database, "resources": [], "warnings": [str(exc)]}

    schemas = [
        row[1] for row in schema_rows
        if isinstance(row[1], str) and row[1].upper() not in _SKIP_SCHEMAS
    ]

    for schema in schemas:
        fq_schema = f"{database}.{schema}"
        for rtype, query_tpl in RESOURCE_QUERIES.items():
            try:
                rows = connector.execute_single(query_tpl.format(schema=fq_schema))
            except Exception as exc:
                msg = f"Could not list {rtype} in schema {schema}: {exc}"
                log.debug(msg)
                warnings.append(msg)
                continue

            for row in rows:
                name = row[1] if len(row) > 1 else ""
                if isinstance(name, str):
                    name = name.split("(")[0].strip().upper()
                created_on = str(row[0]) if row else ""
                # Owner is typically at index 4 for most SHOW commands
                owner = str(row[4]) if len(row) > 4 else ""
                # Comment position varies; last non-empty field is a guess —
                # prefer explicit index 17 (tables) or fall back to empty.
                comment = ""
                if len(row) > 17 and isinstance(row[17], str):
                    comment = row[17]
                elif len(row) > 8 and isinstance(row[8], str):
                    comment = row[8]

                resources.append({
                    "schema": schema,
                    "type": rtype,
                    "name": name,
                    "fqn": f"{schema}.{name}",
                    "created_on": created_on,
                    "owner": owner,
                    "comment": comment,
                })

    return {"database": database, "resources": resources, "warnings": warnings}
