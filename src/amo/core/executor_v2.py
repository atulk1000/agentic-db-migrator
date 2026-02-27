from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import pool, sql

try:
    # Optional — only required when engine.type == "spark"
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F
except Exception:
    SparkSession = None
    F = None


# -----------------------------------------------------------------------------
# V2: Production-style Migration Orchestrator
# -----------------------------------------------------------------------------

ARCHIVED_SUFFIX = "_archive"
TABLE_SUFFIX = "_table"

DEFAULT_SYSTEM_TABLES = {
    "spatial_ref_sys",
    "geometry_columns",
    "geography_columns",
    "raster_columns",
    "raster_overviews",
}


def _dsn(db_cfg: Dict[str, Any]) -> Dict[str, Any]:
    # For psycopg2 pool params
    return dict(
        host=db_cfg["host"],
        port=db_cfg.get("port", 5432),
        database=db_cfg["database"],
        user=db_cfg["user"],
        password=db_cfg["password"],
        keepalives=1,
        keepalives_idle=30,
        keepalives_interval=10,
        keepalives_count=5,
    )


def _fq(schema: str, name: str) -> sql.SQL:
    return sql.SQL("{}.{}").format(sql.Identifier(schema), sql.Identifier(name))


def _read_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text())


def _write_json(path: str | Path, obj: Dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(obj, indent=2, sort_keys=True))


# -----------------------------------------------------------------------------
# Fetch SQL (mirrors your large script capabilities)
# -----------------------------------------------------------------------------

FETCH_TABLES_SQL = """
SELECT table_name
FROM information_schema.tables
WHERE table_schema = %s AND table_type = 'BASE TABLE'
ORDER BY table_name
"""

FETCH_COLUMNS_SQL = """
SELECT column_name, data_type, udt_name
FROM information_schema.columns
WHERE table_schema = %s AND table_name = %s
ORDER BY ordinal_position
"""

FETCH_PRIMARY_KEYS_SQL = """
SELECT
    tc.constraint_name,
    tc.table_schema,
    tc.table_name,
    kcu.column_name,
    kcu.ordinal_position
FROM information_schema.table_constraints tc
JOIN information_schema.key_column_usage kcu
    ON tc.constraint_name = kcu.constraint_name
    AND tc.table_schema = kcu.table_schema
    AND tc.table_name = kcu.table_name
WHERE tc.constraint_type = 'PRIMARY KEY'
  AND tc.table_schema = %s
ORDER BY tc.table_name, tc.constraint_name, kcu.ordinal_position
"""

FETCH_MATVIEWS_SQL = """
SELECT m.matviewname,
       pg_get_viewdef(('"' || ns.nspname || '"."' || m.matviewname || '"')::regclass, true) AS definition
FROM pg_matviews m
JOIN pg_namespace ns ON m.schemaname = ns.nspname
WHERE ns.nspname = %s
"""

FETCH_MV_INDEXES_SQL = """
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
"""

FETCH_USER_INDEXES_SQL = """
WITH parents AS (
  SELECT c.oid, c.relname
  FROM pg_class c
  JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE n.nspname = %(schema)s
    AND c.relname = ANY(%(tables)s::text[])
)
SELECT n.nspname AS schema_name,
       p.relname AS table_name,
       i.relname AS index_name,
       pg_get_indexdef(i.oid) AS index_definition,
       CASE WHEN ix.indisclustered
            THEN format('CLUSTER %%I.%%I USING %%I', n.nspname, p.relname, i.relname)
       END AS cluster_statement
FROM parents p
JOIN pg_class t ON t.oid = p.oid
JOIN pg_namespace n ON n.oid = t.relnamespace
JOIN pg_index ix ON ix.indrelid = p.oid
JOIN pg_class i ON i.oid = ix.indexrelid
WHERE i.relkind IN ('i','I')
  AND NOT ix.indisprimary
  AND NOT EXISTS (SELECT 1 FROM pg_constraint c WHERE c.conindid = i.oid)
ORDER BY p.relname, i.relname
"""

FETCH_UDFS_SQL = """
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
"""


# -----------------------------------------------------------------------------
# Core class
# -----------------------------------------------------------------------------

@dataclass
class MigrationPolicy:
    include_schemas: List[str]
    exclude_schemas: List[str]
    exclude_tables: List[str]
    exclude_suffixes: List[str]
    system_tables: set


class MigrationOrchestrator:
    def __init__(self, cfg: Dict[str, Any]):
        self.cfg = cfg
        self.engine_type = cfg.get("engine", {}).get("type", "copy")

        pol = cfg.get("migration", {})
        self.policy = MigrationPolicy(
            include_schemas=pol.get("include_schemas", []),
            exclude_schemas=pol.get("exclude_schemas", []),
            exclude_tables=pol.get("exclude_tables", []),
            exclude_suffixes=pol.get("exclude_suffixes", [ARCHIVED_SUFFIX, TABLE_SUFFIX]),
            system_tables=set(pol.get("system_tables", list(DEFAULT_SYSTEM_TABLES))),
        )

        self.src_pool = pool.SimpleConnectionPool(1, 20, **_dsn(cfg["source"]))
        self.tgt_pool = pool.SimpleConnectionPool(1, 20, **_dsn(cfg["target"]))

        self.spark = None
        if self.engine_type == "spark":
            if SparkSession is None:
                raise RuntimeError("Spark engine requested but pyspark is not installed / available.")
            self.spark = SparkSession.builder.appName("AgenticDBMigrator").getOrCreate()

    # -------- lifecycle --------
    def close(self):
        try:
            if self.spark:
                self.spark.stop()
        except Exception:
            pass
        self.src_pool.closeall()
        self.tgt_pool.closeall()

    # -------- connections --------
    def _src(self):
        return self.src_pool.getconn()

    def _tgt(self):
        return self.tgt_pool.getconn()

    def _put_src(self, c, close=False):
        self.src_pool.putconn(c, close=close)

    def _put_tgt(self, c, close=False):
        self.tgt_pool.putconn(c, close=close)

    # -------- utilities --------
    def _fetch(self, conn, q: str, params: Tuple[Any, ...]) -> List[tuple]:
        with conn.cursor() as cur:
            cur.execute(q, params)
            return cur.fetchall()

    def _exec(self, conn, q: str, params: Optional[Tuple[Any, ...]] = None) -> None:
        with conn.cursor() as cur:
            cur.execute(q, params or ())

    # -----------------------------------------------------------------------------
    # V2 capabilities (mirrors your big script categories)
    # -----------------------------------------------------------------------------

    def set_session_settings(self, conn) -> None:
        conn.autocommit = True
        with conn.cursor() as cur:
            # keep minimal, configurable
            settings = self.cfg.get("postgres_session_settings", {
                "SET synchronous_commit TO OFF;": None,
                "SET work_mem = '256MB';": None,
                "SET maintenance_work_mem = '1GB';": None,
            })
            for stmt in settings:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass

    def list_tables(self, schema: str) -> List[str]:
        c = self._src()
        try:
            rows = self._fetch(c, FETCH_TABLES_SQL, (schema,))
            return [r[0] for r in rows]
        finally:
            self._put_src(c)

    def get_columns(self, schema: str, table: str) -> List[Tuple[str, str, str]]:
        c = self._src()
        try:
            return self._fetch(c, FETCH_COLUMNS_SQL, (schema, table))
        finally:
            self._put_src(c)

    def copy_table(self, schema: str, table: str) -> None:
        """
        Engine adapter:
          - copy: psycopg2 COPY streaming
          - spark: Spark JDBC (optional)
        """
        if self.engine_type == "copy":
            self._copy_table_psycopg2(schema, table)
        elif self.engine_type == "spark":
            self._copy_table_spark(schema, table)
        else:
            raise RuntimeError(f"Unknown engine.type: {self.engine_type}")

    def _copy_table_psycopg2(self, schema: str, table: str) -> None:
        src = self._src()
        tgt = self._tgt()
        try:
            self.set_session_settings(src)
            self.set_session_settings(tgt)

            tgt.autocommit = False
            with tgt.cursor() as cur:
                cur.execute(sql.SQL("TRUNCATE TABLE {}").format(_fq(schema, table)))

            with src.cursor() as src_cur, tgt.cursor() as tgt_cur:
                import io
                buf = io.StringIO()
                src_cur.copy_expert(
                    f'COPY (SELECT * FROM "{schema}"."{table}") TO STDOUT WITH (FORMAT CSV)',
                    buf,
                )
                buf.seek(0)
                tgt_cur.copy_expert(
                    f'COPY "{schema}"."{table}" FROM STDIN WITH (FORMAT CSV)',
                    buf,
                )

            tgt.commit()

        except Exception:
            try:
                tgt.rollback()
            except Exception:
                pass
            raise
        finally:
            self._put_src(src)
            self._put_tgt(tgt)

    def _copy_table_spark(self, schema: str, table: str) -> None:
        """
        Optional: Spark JDBC engine. Keep this as "advanced path" in README.
        """
        assert self.spark is not None
        jdbc_src = self.cfg["source"]["jdbc_url"]
        jdbc_tgt = self.cfg["target"]["jdbc_url"]

        src_props = self.cfg["source"].get("jdbc_properties", {})
        tgt_props = self.cfg["target"].get("jdbc_properties", {})

        df = (
            self.spark.read.format("jdbc")
            .option("url", jdbc_src)
            .option("dbtable", f'"{schema}"."{table}"')
            .options(**src_props)
            .load()
        )

        (
            df.write.format("jdbc")
            .option("url", jdbc_tgt)
            .option("dbtable", f'"{schema}"."{table}"')
            .options(**tgt_props)
            .mode("overwrite")
            .save()
        )

    # --- Geometry conversion (EWKB bytea → geometry) ---
    def convert_bytea_to_geometry(self, schema: str, table: str, columns: List[Tuple[str, str, str]]) -> None:
        """
        If geometry columns were extracted as EWKB into bytea/text, convert back.
        In V2 you can call this after Spark JDBC writes (or if you choose EWKB extraction).
        """
        src = self._src()
        tgt = self._tgt()
        try:
            tgt.autocommit = False
            with src.cursor() as src_cur, tgt.cursor() as tgt_cur:
                TM_SQL = """
                SELECT postgis.postgis_typmod_type(a.atttypmod),
                       postgis.postgis_typmod_srid(a.atttypmod)
                  FROM pg_attribute a
                  JOIN pg_class c ON a.attrelid = c.oid
                  JOIN pg_namespace n ON c.relnamespace = n.oid
                 WHERE n.nspname = %s AND c.relname = %s AND a.attname = %s
                   AND NOT a.attisdropped;
                """
                for col_name, _, udt in columns:
                    if udt != "geometry":
                        continue

                    src_cur.execute(TM_SQL, (schema, table, col_name))
                    dev_gtype, dev_srid = src_cur.fetchone() or (None, None)

                    if dev_gtype and dev_srid and dev_srid > 0:
                        type_clause = sql.SQL("postgis.geometry({}, {})").format(
                            sql.SQL(dev_gtype), sql.Literal(int(dev_srid))
                        )
                    else:
                        type_clause = sql.SQL("postgis.geometry")

                    tgt_cur.execute(
                        sql.SQL("""
                            ALTER TABLE {} 
                            ALTER COLUMN {} TYPE {} 
                            USING postgis.ST_GeomFromEWKB({}::bytea)
                        """).format(
                            _fq(schema, table),
                            sql.Identifier(col_name),
                            type_clause,
                            sql.Identifier(col_name),
                        )
                    )

            tgt.commit()
        except Exception:
            tgt.rollback()
            raise
        finally:
            self._put_src(src)
            self._put_tgt(tgt)

    # --- Materialized views, indexes, PKs, UDFs, grants, partition parents ---
    # For GitHub polish: keep them as methods, invoked by config flags.

    def migrate_schema_full(self, schema: str) -> Dict[str, Any]:
        """
        Full orchestration: tables + (optional) partitions + indexes + matviews + PKs + UDFs + grants.
        This mirrors your big script shape.
        """
        if schema in self.policy.exclude_schemas:
            return {"schema": schema, "skipped": True, "reason": "excluded"}

        tables = self.list_tables(schema)
        tables = [
            t for t in tables
            if t not in self.policy.system_tables
            and t not in self.policy.exclude_tables
            and not any(t.endswith(suf) for suf in self.policy.exclude_suffixes)
        ]

        # V2: you can additionally detect partition children/parents and handle them specially
        # (omitted here for brevity, but stubbed as a place to plug it in).
        results = {"schema": schema, "tables": [], "matviews": [], "indexes": [], "pks": [], "udfs": []}

        for t in tables:
            self.copy_table(schema, t)
            results["tables"].append(t)

        # TODO: migrate user-defined indexes on tables
        # TODO: migrate PK constraints
        # TODO: migrate materialized views (create staging table -> MV -> MV indexes)
        # TODO: migrate UDFs (postgis schema or configurable schemas)
        # TODO: apply grants
        return results


# -----------------------------------------------------------------------------
# V2 Entry Point
# -----------------------------------------------------------------------------

def execute_v2(cfg: Dict[str, Any], plan_path: Optional[str] = None, state_path: str = "state.json") -> None:
    """
    V2 can run in two modes:
      - plan-driven: iterate plan steps
      - schema-driven: if no plan, migrate schemas from config
    """
    orch = MigrationOrchestrator(cfg)
    state_file = Path(state_path)

    state = _read_json(state_file) if state_file.exists() else {"completed": {}, "started_at": time.time()}
    completed = state.get("completed", {})

    try:
        if plan_path:
            plan = _read_json(plan_path)
            steps = plan.get("steps", [])
            for step in steps:
                step_id = step.get("id") or f"{step.get('schema')}.{step.get('table')}.{step.get('op')}"
                if step_id in completed:
                    print(f"↩️  Skipping {step_id}")
                    continue

                op = step.get("op")
                if op == "copy_table":
                    orch.copy_table(step["schema"], step["table"])
                elif op == "migrate_schema_full":
                    orch.migrate_schema_full(step["schema"])
                else:
                    raise RuntimeError(f"Unknown op: {op}")

                completed[step_id] = {"ok": True, "ts": time.time()}
                state["completed"] = completed
                state["updated_at"] = time.time()
                _write_json(state_file, state)

        else:
            schemas = cfg.get("migration", {}).get("include_schemas", [])
            for schema in schemas:
                step_id = f"schema_full:{schema}"
                if step_id in completed:
                    print(f"↩️  Skipping {step_id}")
                    continue
                orch.migrate_schema_full(schema)
                completed[step_id] = {"ok": True, "ts": time.time()}
                state["completed"] = completed
                state["updated_at"] = time.time()
                _write_json(state_file, state)

    finally:
        orch.close()
