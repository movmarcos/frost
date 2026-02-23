"""Snowflake Cortex AI-powered error suggestions.

When a deployment error occurs, frost can ask Snowflake Cortex to
analyse the failed SQL and error message, then return a short,
actionable fix suggestion that is rendered in the terminal output.

The call goes through the *same* Snowflake session that is already
open, so no extra credentials are needed.  If Cortex is not available
on the account (feature not enabled, model not provisioned, etc.) the
call fails silently and frost falls back to the static hint table.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from frost.connector import SnowflakeConnector
    from frost.reporter import DeployError

log = logging.getLogger("frost")

_DEFAULT_MODEL = "mistral-large2"

# Maximum number of errors to enrich -- keeps deploy exit fast.
_MAX_SUGGESTIONS = 3

_PROMPT = """\
You are a Snowflake SQL expert helping a developer fix a DDL deployment error.

Object : {fqn} ({object_type})
File   : {file_path}

SQL that was executed:
{sql}

Snowflake error:
{error}

Provide a short, actionable fix (1-3 sentences).
- Focus on what to change and where.
- If the fix involves SQL, show the corrected snippet.
- Do NOT repeat the error message.
- Do NOT use markdown formatting.\
"""


def cortex_suggest(
    connector: "SnowflakeConnector",
    fqn: str,
    object_type: str,
    file_path: str,
    sql: str,
    error_message: str,
    model: str = _DEFAULT_MODEL,
) -> Optional[str]:
    """Ask Snowflake Cortex for a suggestion to fix a deployment error.

    Returns the suggestion text, or *None* if Cortex is unavailable or
    the call fails for any reason.
    """
    prompt = _PROMPT.format(
        fqn=fqn,
        object_type=object_type,
        file_path=file_path,
        sql=sql[:3000],       # trim very long SQL
        error=error_message[:1000],
    )

    # Defensive: strip the dollar-quote delimiter if it appears in content
    prompt = prompt.replace("$frost_prompt$", "$frost prompt$")

    cortex_sql = (
        f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', "
        f"$frost_prompt${prompt}$frost_prompt$)"
    )

    try:
        log.info("  Cortex  ->  asking %s for a fix suggestion ...", model)
        rows = connector.execute_single(cortex_sql)
        if rows and rows[0]:
            suggestion = str(rows[0][0]).strip().strip('"').strip("'").strip()
            if suggestion:
                log.info("  Cortex  <-  suggestion received")
                return suggestion
    except Exception as exc:
        # Cortex not enabled, model not provisioned, permission issue, etc.
        log.debug("Cortex suggestion unavailable: %s", exc)

    return None


def enrich_errors_with_cortex(
    connector: "SnowflakeConnector",
    errors: List["DeployError"],
    model: str = _DEFAULT_MODEL,
) -> int:
    """Enrich up to *_MAX_SUGGESTIONS* deploy errors with Cortex hints.

    Mutates each ``DeployError.ai_suggestion`` in place.
    Returns the number of suggestions successfully attached.
    """
    enriched = 0
    for err in errors[:_MAX_SUGGESTIONS]:
        suggestion = cortex_suggest(
            connector,
            fqn=err.fqn,
            object_type=err.object_type,
            file_path=err.file_path,
            sql=err.sql,
            error_message=err.error_message,
            model=model,
        )
        if suggestion:
            err.ai_suggestion = suggestion
            enriched += 1

    return enriched
