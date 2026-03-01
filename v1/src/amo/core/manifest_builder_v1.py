from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2


def _dsn_from_cfg(db_cfg: Dict[str, Any]) -> str:
    """
    Build a psycopg2 DSN from config dict keys:
    host, port, database, user, password
    """
    return (
        f"host={db_cfg['host']} "
        f"port={db_cfg['port']} "
        f"dbname={db_cfg['database']} "
        f"user={db_cfg['user']} "
        f"password={db_cfg['password']}"
    )


def _fetchall(cur, query: str, params: Tuple[Any, ...] = ()) -> List[tuple]:
    cur.execute(query, params)
    return cur.fetchall()


def _table_exists(cur, schema: str, table: str) -> bool:
    cur.execute(
        """
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = %s AND table_name = %s
        """,
        (schema, table),
    )
    return cur.fetchone() is not None


def _get_tables_in_schema(cur, schema: str) -> List[str]:
    rows = _fetchall(
        cur,
        """
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = %s
          AND table_type = 'BASE TABLE'
        ORDER BY table_name
        """,
        (schema,),
    )
    return [r[0] for r in rows]


def _get_columns(cur, schema: str, table: str) -> List[Dict[str, str]]:
    rows = _fetchall(
        cur,
        """
        SELECT
          column_name,
          data_type,
          udt_name,
          is_nullable
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """,
        (schema, table),
    )
    return [
        {
            "name": r[0],
            "data_type": r[1],
            "udt_name": r[2],
            "nullable": (r[3] == "YES"),
        }
        for r in rows
    ]


def _get_primary_key_columns(cur, schema: str, table: str) -> List[str]:
    rows = _fetchall(
        cur,
        """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
          ON tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'PRIMARY KEY'
          AND tc.table_schema = %s
          AND tc.table_name = %s
        ORDER BY kcu.ordinal_position
        """,
        (schema, table),
    )
    return [r[0] for r in rows]


def _estimate_rows_pg_stats(cur, schema: str, table: str) -> Optional[int]:
    """
    Fast estimate from pg_stat_all_tables.n_live_tup.
    Returns None if stats are missing.
    """
    cur.execute(
        """
        SELECT s.n_live_tup::bigint
        FROM pg_stat_all_tables s
        WHERE s.schemaname = %s AND s.relname = %s
        """,
        (schema, table),
    )
    row = cur.fetchone()
    if not row:
        return None
    val = row[0]
    return int(val) if val is not None else None


def _has_geometry(columns: List[Dict[str, Any]]) -> bool:
    # PostGIS geometry columns appear as udt_name = 'geometry'
    return any(c.get("udt_name") == "geometry" for c in columns)


def build_manifest(cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Discover source DB metadata per config and return a manifest dict.
    """
    migration_cfg = cfg.get("migration", {})
    include_schemas: List[str] = migration_cfg.get("include_schemas", [])
    exclude_schemas: List[str] = migration_cfg.get("exclude_schemas", [])

    exclude_tables: List[str] = migration_cfg.get("exclude_tables", [])
    exclude_suffixes: List[str] = migration_cfg.get("exclude_suffixes", [])

    src_cfg = cfg["source"]
    dsn = _dsn_from_cfg(src_cfg)

    discovered: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    with psycopg2.connect(dsn) as conn:
        with conn.cursor() as cur:
            for schema in include_schemas:
                if schema in exclude_schemas:
                    continue

                try:
                    tables = _get_tables_in_schema(cur, schema)
                except Exception as e:
                    errors.append({"schema": schema, "error": str(e)})
                    continue

                for table in tables:
                    if table in exclude_tables:
                        continue
                    if any(table.endswith(suf) for suf in exclude_suffixes):
                        continue

                    try:
                        cols = _get_columns(cur, schema, table)
                        pk_cols = _get_primary_key_columns(cur, schema, table)
                        est_rows = _estimate_rows_pg_stats(cur, schema, table)

                        discovered.append(
                            {
                                "schema": schema,
                                "table": table,
                                "estimated_rows": est_rows,
                                "primary_key": pk_cols,
                                "has_geometry": _has_geometry(cols),
                                "columns": cols,
                            }
                        )
                    except Exception as e:
                        errors.append({"schema": schema, "table": table, "error": str(e)})

    manifest = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "host": src_cfg.get("host"),
            "port": src_cfg.get("port"),
            "database": src_cfg.get("database"),
        },
        "include_schemas": include_schemas,
        "tables": discovered,
        "errors": errors,
    }
    return manifest


def write_manifest(manifest: Dict[str, Any], out_path: str | Path) -> None:
    p = Path(out_path)
    p.write_text(json.dumps(manifest, indent=2, sort_keys=True))
