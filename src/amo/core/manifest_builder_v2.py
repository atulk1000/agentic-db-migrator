from __future__ import annotations

import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2

DEFAULT_SYSTEM_TABLES = {
    "spatial_ref_sys",
    "geometry_columns",
    "geography_columns",
    "raster_columns",
    "raster_overviews",
}

_NEXTVAL_RE = re.compile(r"nextval\('([^']+)'\s*(?:::regclass)?\)", re.IGNORECASE)


def _dsn_from_cfg(db_cfg: Dict[str, Any]) -> str:
    return (
        f"host={db_cfg['host']} "
        f"port={db_cfg.get('port', 5432)} "
        f"dbname={db_cfg['database']} "
        f"user={db_cfg['user']} "
        f"password={db_cfg['password']}"
    )


def _fetchall(cur, query: str, params: Tuple[Any, ...] = ()) -> List[tuple]:
    cur.execute(query, params)
    return cur.fetchall()


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


def _get_table_relkind(cur, schema: str, table: str) -> Optional[str]:
    cur.execute(
        """
        SELECT c.relkind
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    )
    row = cur.fetchone()
    return row[0] if row else None


def _get_total_relation_size(cur, schema: str, table: str) -> Optional[int]:
    # regclass accepts quoted identifier strings
    qname = f'"{schema}"."{table}"'
    cur.execute("SELECT pg_total_relation_size(%s::regclass)", (qname,))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _estimate_rows_pg_stats(cur, schema: str, table: str) -> Optional[int]:
    cur.execute(
        """
        SELECT s.n_live_tup::bigint
        FROM pg_stat_all_tables s
        WHERE s.schemaname = %s AND s.relname = %s
        """,
        (schema, table),
    )
    row = cur.fetchone()
    if not row or row[0] is None:
        return None
    return int(row[0])


def _get_columns_pg(cur, schema: str, table: str) -> List[Dict[str, Any]]:
    """
    DDL-quality column metadata:
      - type_sql: pg_catalog.format_type
      - default_sql: pg_get_expr(adbin, adrelid)
      - not_null
      - udt_name: pg_type.typname (e.g., geometry, int4, text, etc.)
      - attidentity: '' | 'a' | 'd' (identity column)
    """
    rows = _fetchall(
        cur,
        """
        SELECT
            a.attname AS col_name,
            pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_sql,
            t.typname AS udt_name,
            a.attnotnull AS not_null,
            a.attidentity AS attidentity,
            pg_get_expr(ad.adbin, ad.adrelid) AS default_sql
        FROM pg_attribute a
        JOIN pg_class c ON c.oid = a.attrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_type t ON t.oid = a.atttypid
        LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
        WHERE n.nspname = %s
          AND c.relname = %s
          AND a.attnum > 0
          AND NOT a.attisdropped
        ORDER BY a.attnum
        """,
        (schema, table),
    )

    cols: List[Dict[str, Any]] = []
    for col_name, type_sql, udt_name, not_null, attidentity, default_sql in rows:
        seqs = _NEXTVAL_RE.findall(default_sql or "")
        cols.append(
            {
                "name": col_name,
                "type_sql": type_sql,
                "udt_name": udt_name,
                "not_null": bool(not_null),
                "attidentity": attidentity or "",
                "default_sql": default_sql,
                "nextval_sequences": seqs,  # may be empty
            }
        )
    return cols


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


def _has_geometry(cols: List[Dict[str, Any]]) -> bool:
    return any(c.get("udt_name") == "geometry" for c in cols)


def _get_partition_info(cur, schema: str, table: str) -> Dict[str, Any]:
    """
    Returns:
      {
        "is_partition_parent": bool,
        "partition_key": str|None,
        "children": [{"schema":..., "table":..., "bound":...}, ...]
      }
    """
    relkind = _get_table_relkind(cur, schema, table)
    is_parent = relkind == "p"

    if not is_parent:
        return {"is_partition_parent": False, "partition_key": None, "children": []}

    # key
    cur.execute(
        """
        SELECT pg_get_partkeydef(c.oid)
        FROM pg_class c
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = %s AND c.relname = %s
        """,
        (schema, table),
    )
    row = cur.fetchone()
    partkey = row[0] if row else None

    # children + bounds
    rows = _fetchall(
        cur,
        """
        SELECT cn.nspname AS child_schema,
               child.relname AS child_table,
               pg_get_expr(child.relpartbound, child.oid) AS bound
        FROM pg_inherits i
        JOIN pg_class parent ON parent.oid = i.inhparent
        JOIN pg_namespace pn ON pn.oid = parent.relnamespace
        JOIN pg_class child ON child.oid = i.inhrelid
        JOIN pg_namespace cn ON cn.oid = child.relnamespace
        WHERE pn.nspname = %s
          AND parent.relname = %s
        ORDER BY cn.nspname, child.relname
        """,
        (schema, table),
    )

    children = [{"schema": r[0], "table": r[1], "bound": r[2]} for r in rows]
    return {"is_partition_parent": True, "partition_key": partkey, "children": children}


def _get_foreign_keys(cur, schema: str, table: str) -> List[Dict[str, Any]]:
    """
    Returns list of FK constraints on (schema.table):
      {name, definition, ref_schema, ref_table}
    definition is pg_get_constraintdef(...) (e.g. "FOREIGN KEY (...) REFERENCES ...")
    """
    rows = _fetchall(
        cur,
        """
        SELECT
            con.conname AS name,
            rn.nspname AS ref_schema,
            rr.relname AS ref_table,
            pg_get_constraintdef(con.oid, true) AS definition
        FROM pg_constraint con
        JOIN pg_class sr ON sr.oid = con.conrelid
        JOIN pg_namespace sn ON sn.oid = sr.relnamespace
        JOIN pg_class rr ON rr.oid = con.confrelid
        JOIN pg_namespace rn ON rn.oid = rr.relnamespace
        WHERE con.contype = 'f'
          AND sn.nspname = %s
          AND sr.relname = %s
        ORDER BY con.conname
        """,
        (schema, table),
    )
    return [{"name": r[0], "ref_schema": r[1], "ref_table": r[2], "definition": r[3]} for r in rows]


def _get_non_pk_indexes(cur, schema: str, table: str) -> List[Dict[str, Any]]:
    """
    Returns non-PK, non-constraint-backed index definitions for a table.
    """
    rows = _fetchall(
        cur,
        """
        SELECT
            i.relname AS index_name,
            pg_get_indexdef(i.oid) AS index_definition,
            ix.indisclustered AS is_clustered
        FROM pg_class t
        JOIN pg_namespace n ON n.oid = t.relnamespace
        JOIN pg_index ix ON ix.indrelid = t.oid
        JOIN pg_class i ON i.oid = ix.indexrelid
        WHERE n.nspname = %s
          AND t.relname = %s
          AND NOT ix.indisprimary
          AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.oid)
        ORDER BY i.relname
        """,
        (schema, table),
    )

    idxs = []
    for index_name, index_definition, is_clustered in rows:
        cluster_stmt = None
        if is_clustered:
            cluster_stmt = f'CLUSTER "{schema}"."{table}" USING "{index_name}"'
        idxs.append(
            {
                "index_name": index_name,
                "index_definition": index_definition,
                "cluster_statement": cluster_stmt,
            }
        )
    return idxs


def _get_matviews(cur, schema: str) -> List[Dict[str, Any]]:
    rows = _fetchall(
        cur,
        """
        SELECT m.matviewname,
               pg_get_viewdef(('"' || ns.nspname || '"."' || m.matviewname || '"')::regclass, true) AS definition
        FROM pg_matviews m
        JOIN pg_namespace ns ON m.schemaname = ns.nspname
        WHERE ns.nspname = %s
        ORDER BY m.matviewname
        """,
        (schema,),
    )
    return [{"schema": schema, "name": r[0], "definition": r[1]} for r in rows]


def _get_matview_indexes(cur, schema: str) -> List[Dict[str, Any]]:
    rows = _fetchall(
        cur,
        """
        SELECT
            ns.nspname AS schema_name,
            t.relname AS view_name,
            i.relname AS index_name,
            pg_get_indexdef(i.oid) AS index_definition,
            CASE WHEN ix.indisclustered
                 THEN 'CLUSTER ' || ns.nspname || '."' || t.relname || '" USING ' || i.relname
                 ELSE NULL
            END AS cluster_statement
        FROM pg_class t
        JOIN pg_namespace ns ON t.relnamespace = ns.oid
        JOIN pg_index ix ON t.oid = ix.indrelid
        JOIN pg_class i ON i.oid = ix.indexrelid
        WHERE t.relkind = 'm'
          AND i.relkind = 'i'
          AND NOT ix.indisprimary
          AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.oid)
          AND ns.nspname = %s
        ORDER BY t.relname, i.relname
        """,
        (schema,),
    )
    return [
        {
            "schema": r[0],
            "matview": r[1],
            "index_name": r[2],
            "index_definition": r[3],
            "cluster_statement": r[4],
        }
        for r in rows
    ]


def _get_udfs(cur, schema: str) -> List[Dict[str, Any]]:
    rows = _fetchall(
        cur,
        """
        SELECT n.nspname AS schema_name,
               p.proname AS function_name,
               pg_get_functiondef(p.oid) AS create_statement
        FROM pg_proc p
        JOIN pg_namespace n ON p.pronamespace = n.oid
        WHERE n.nspname = %s
          AND n.nspname !~ '^pg_'
          AND n.nspname <> 'information_schema'
          AND p.prokind = 'f'
          AND NOT EXISTS (
              SELECT 1
              FROM pg_depend d
              JOIN pg_extension e ON d.refobjid = e.oid
              WHERE d.classid = (SELECT oid FROM pg_class WHERE relname = 'pg_proc')
                AND d.objid = p.oid
                AND e.extname = 'postgis'
          )
        ORDER BY p.proname
        """,
        (schema,),
    )
    return [{"schema": r[0], "name": r[1], "create_statement": r[2]} for r in rows]


def build_manifest(cfg: Dict[str, Any]) -> Dict[str, Any]:
    migration_cfg = cfg.get("migration", {})
    include_schemas: List[str] = migration_cfg.get("include_schemas", [])
    exclude_schemas: List[str] = migration_cfg.get("exclude_schemas", [])
    exclude_tables: List[str] = migration_cfg.get("exclude_tables", [])
    exclude_suffixes: List[str] = migration_cfg.get("exclude_suffixes", [])
    system_tables = set(migration_cfg.get("system_tables", list(DEFAULT_SYSTEM_TABLES)))

    src_cfg = cfg["source"]
    dsn = _dsn_from_cfg(src_cfg)

    discovered_tables: List[Dict[str, Any]] = []
    discovered_matviews: List[Dict[str, Any]] = []
    discovered_mv_indexes: List[Dict[str, Any]] = []
    discovered_udfs: List[Dict[str, Any]] = []
    errors: List[Dict[str, str]] = []

    with psycopg2.connect(dsn) as conn:
        conn.autocommit = True
        with conn.cursor() as cur:
            for schema in include_schemas:
                if schema in exclude_schemas:
                    continue

                try:
                    tables = _get_tables_in_schema(cur, schema)
                except Exception as e:
                    errors.append({"schema": schema, "error": str(e)})
                    continue

                # schema-level objects
                try:
                    discovered_matviews.extend(_get_matviews(cur, schema))
                    discovered_mv_indexes.extend(_get_matview_indexes(cur, schema))
                    # UDFs are optional: include only if asked
                    if bool(cfg.get("migration", {}).get("include_udfs", False)):
                        discovered_udfs.extend(_get_udfs(cur, schema))
                except Exception as e:
                    errors.append({"schema": schema, "error": f"schema objects: {e}"})

                for table in tables:
                    if table in system_tables:
                        continue
                    if table in exclude_tables:
                        continue
                    if any(table.endswith(suf) for suf in exclude_suffixes):
                        continue

                    try:
                        cols = _get_columns_pg(cur, schema, table)
                        pk_cols = _get_primary_key_columns(cur, schema, table)
                        est_rows = _estimate_rows_pg_stats(cur, schema, table)
                        est_bytes = _get_total_relation_size(cur, schema, table)

                        part = _get_partition_info(cur, schema, table)
                        fks = _get_foreign_keys(cur, schema, table)
                        idxs = _get_non_pk_indexes(cur, schema, table)

                        discovered_tables.append(
                            {
                                "schema": schema,
                                "table": table,
                                "estimated_rows": est_rows,
                                "estimated_bytes": est_bytes,
                                "primary_key": pk_cols,
                                "has_geometry": _has_geometry(cols),
                                "columns": cols,
                                "partition": part,
                                "foreign_keys": fks,
                                "indexes": idxs,
                            }
                        )
                    except Exception as e:
                        errors.append({"schema": schema, "table": table, "error": str(e)})

    return {
        "version": "v2",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "source": {
            "host": src_cfg.get("host"),
            "port": src_cfg.get("port"),
            "database": src_cfg.get("database"),
        },
        "include_schemas": include_schemas,
        "tables": discovered_tables,
        "matviews": discovered_matviews,
        "matview_indexes": discovered_mv_indexes,
        "udfs": discovered_udfs,
        "errors": errors,
    }


def write_manifest(manifest: Dict[str, Any], out_path: str | Path) -> None:
    Path(out_path).write_text(json.dumps(manifest, indent=2, sort_keys=True))
