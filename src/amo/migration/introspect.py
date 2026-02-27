from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import psycopg2


def dsn_from_cfg(db_cfg: Dict[str, Any]) -> str:
    return (
        f"host={db_cfg['host']} port={db_cfg.get('port', 5432)} dbname={db_cfg['database']} "
        f"user={db_cfg['user']} password={db_cfg['password']}"
    )


def list_base_tables(conn, schema: str) -> List[str]:
    """
    Returns base tables only (not views/matviews).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s
              AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """,
            (schema,),
        )
        return [r[0] for r in cur.fetchall()]


def get_columns(conn, schema: str, table: str) -> List[Dict[str, str]]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT column_name, data_type, udt_name
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (schema, table),
        )
        cols = []
        for name, dtype, udt in cur.fetchall():
            cols.append({"name": name, "data_type": dtype, "udt_name": udt})
        return cols


def estimate_rows(conn, schema: str, table: str) -> Optional[int]:
    """
    Uses pg_stat_all_tables.n_live_tup when available.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT s.n_live_tup::bigint
            FROM pg_stat_all_tables s
            WHERE s.relid = to_regclass(%s)
            """,
            (f'"{schema}"."{table}"',),
        )
        row = cur.fetchone()
        if not row:
            return None
        return int(row[0]) if row[0] is not None else None


def count_rows(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f'SELECT COUNT(*) FROM "{schema}"."{table}";')
        return int(cur.fetchone()[0])


def discover_schema(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Builds a manifest-like object for a schema from the SOURCE db.
    """
    source = cfg["source"]
    schema = cfg.get("schema") or cfg.get("schemas", [None])[0]
    if not schema:
        raise ValueError("Config must include `schema:` or `schemas:` for discovery.")

    include_tables = set(cfg.get("policy", {}).get("include_tables", []) or [])
    exclude_tables = set(cfg.get("policy", {}).get("exclude_tables", []) or [])

    dsn = dsn_from_cfg(source)

    out_tables: List[Dict[str, Any]] = []
    errors: List[str] = []

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        try:
            tables = list_base_tables(conn, schema)
        except Exception as e:
            raise RuntimeError(f"Failed listing tables for schema={schema}: {e}") from e

        for t in tables:
            if include_tables and t not in include_tables:
                continue
            if t in exclude_tables:
                continue

            try:
                cols = get_columns(conn, schema, t)
                est = estimate_rows(conn, schema, t)
                out_tables.append(
                    {
                        "schema": schema,
                        "table": t,
                        "columns": cols,
                        "estimated_rows": est,
                    }
                )
            except Exception as e:
                errors.append(f"{schema}.{t}: {e}")

    return {
        "source": {"host": source.get("host"), "database": source.get("database")},
        "schema": schema,
        "tables": out_tables,
        "errors": errors,
    }
