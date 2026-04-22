from __future__ import annotations

import json
import re
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
from psycopg2 import pool, sql
from psycopg2 import extensions as ext


ARCHIVED_SUFFIX = "_archive"
TABLE_SUFFIX = "_table"

DEFAULT_SYSTEM_TABLES = {
    "spatial_ref_sys",
    "geometry_columns",
    "geography_columns",
    "raster_columns",
    "raster_overviews",
}

_NEXTVAL_RE = re.compile(r"nextval\('([^']+)'\s*(?:::regclass)?\)", re.IGNORECASE)


def _dsn(db_cfg: Dict[str, Any]) -> Dict[str, Any]:
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


def _ensure_idle(conn) -> None:
    """
    Ensure connection is not inside a transaction.
    This is critical with connection pools + autocommit flips.
    """
    try:
        if conn.status != ext.STATUS_READY:
            conn.rollback()
    except Exception:
        pass


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

        # Safety flags
        self.allow_destructive = bool(cfg.get("engine", {}).get("allow_destructive", False))
        self.auto_ddl = bool(cfg.get("engine", {}).get("auto_ddl", True))
        self.truncate_first = bool(cfg.get("engine", {}).get("copy", {}).get("truncate_first", True))
        self.spool_dir = cfg.get("engine", {}).get("copy", {}).get("spool_dir")  # optional

        self.verify_inline = bool(cfg.get("engine", {}).get("verify_inline", False))

    # -------- lifecycle --------
    def close(self):
        self.src_pool.closeall()
        self.tgt_pool.closeall()

    # -------- connections --------
    def _src(self):
        c = self.src_pool.getconn()
        _ensure_idle(c)
        return c

    def _tgt(self):
        c = self.tgt_pool.getconn()
        _ensure_idle(c)
        return c

    def _put_src(self, c, close: bool = False):
        # reset pooled connection state
        try:
            _ensure_idle(c)
            c.autocommit = True
        except Exception:
            pass
        self.src_pool.putconn(c, close=close)

    def _put_tgt(self, c, close: bool = False):
        # reset pooled connection state
        try:
            _ensure_idle(c)
            c.autocommit = True
        except Exception:
            pass
        self.tgt_pool.putconn(c, close=close)

    # -------- utilities --------
    def _fetch(self, conn, q: str, params: Tuple[Any, ...]) -> List[tuple]:
        with conn.cursor() as cur:
            cur.execute(q, params)
            return cur.fetchall()

    def copy_table(self, schema: str, table: str, transfer: Optional[Dict[str, Any]] = None) -> None:
        """
        Public method invoked by plan step: op == 'copy_table'
        Ensures schema + table exist (auto-DDL if enabled), then copies data.
        """
        self.ensure_schema(schema)
        self.ensure_table_like_source(schema, table)
        transfer = transfer or {}
        if self.engine_type == "spark_jdbc":
            self._copy_table_spark_jdbc(schema, table, transfer=transfer)
            return
        self._copy_table_psycopg2(schema, table)
    
    def set_session_settings(self, conn) -> None:
        """
        Runs configured SET statements.
        IMPORTANT: caller must already have the desired autocommit mode set.
        """
        with conn.cursor() as cur:
            settings = self.cfg.get(
                "postgres_session_settings",
                {
                    "SET synchronous_commit TO OFF;": None,
                    "SET work_mem = '256MB';": None,
                    "SET maintenance_work_mem = '1GB';": None,
                },
            )
            for stmt in settings:
                try:
                    cur.execute(stmt)
                except Exception:
                    pass

    # -------- existence checks --------
    def schema_exists(self, conn, schema: str) -> bool:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM information_schema.schemata WHERE schema_name=%s", (schema,))
            return cur.fetchone() is not None

    def table_exists(self, conn, schema: str, table: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1
                FROM information_schema.tables
                WHERE table_schema=%s AND table_name=%s
                """,
                (schema, table),
            )
            return cur.fetchone() is not None

    def matview_exists(self, conn, schema: str, name: str) -> bool:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM pg_matviews
                WHERE schemaname=%s AND matviewname=%s
                """,
                (schema, name),
            )
            return cur.fetchone() is not None

    # -------- core ops --------
    def ensure_schema(self, schema: str) -> None:
        tgt = self._tgt()
        try:
            # autocommit mode must be set BEFORE any SQL
            tgt.autocommit = True
            self.set_session_settings(tgt)

            if not self.schema_exists(tgt, schema):
                with tgt.cursor() as cur:
                    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))
        finally:
            self._put_tgt(tgt)

    def _fetch_source_columns_for_ddl(self, src_conn, schema: str, table: str) -> List[Dict[str, Any]]:
        rows = self._fetch(
            src_conn,
            """
            SELECT
                a.attname AS col_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_sql,
                a.attnotnull AS not_null,
                a.attidentity AS attidentity,
                pg_get_expr(ad.adbin, ad.adrelid) AS default_sql
            FROM pg_attribute a
            JOIN pg_class c ON c.oid = a.attrelid
            JOIN pg_namespace n ON n.oid = c.relnamespace
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
        for col_name, type_sql, not_null, attidentity, default_sql in rows:
            cols.append(
                {
                    "name": col_name,
                    "type_sql": type_sql,
                    "not_null": bool(not_null),
                    "attidentity": attidentity or "",
                    "default_sql": default_sql,
                }
            )
        return cols

    def _fetch_source_primary_key(self, src_conn, schema: str, table: str) -> List[str]:
        rows = self._fetch(
            src_conn,
            """
            SELECT a.attname
            FROM pg_constraint con
            JOIN pg_class rel ON rel.oid = con.conrelid
            JOIN pg_namespace n ON n.oid = rel.relnamespace
            JOIN unnest(con.conkey) WITH ORDINALITY AS ck(attnum, ord) ON TRUE
            JOIN pg_attribute a ON a.attrelid = rel.oid AND a.attnum = ck.attnum
            WHERE con.contype = 'p'
              AND n.nspname = %s
              AND rel.relname = %s
            ORDER BY ck.ord
            """,
            (schema, table),
        )
        return [r[0] for r in rows]

    def _fetch_source_partition_parent(self, src_conn, schema: str, table: str) -> Optional[Tuple[str, str]]:
        rows = self._fetch(
            src_conn,
            """
            SELECT pn.nspname, parent.relname
            FROM pg_inherits i
            JOIN pg_class child ON child.oid = i.inhrelid
            JOIN pg_namespace cn ON cn.oid = child.relnamespace
            JOIN pg_class parent ON parent.oid = i.inhparent
            JOIN pg_namespace pn ON pn.oid = parent.relnamespace
            WHERE cn.nspname = %s
              AND child.relname = %s
            ORDER BY pn.nspname, parent.relname
            LIMIT 1
            """,
            (schema, table),
        )
        return (rows[0][0], rows[0][1]) if rows else None

    def _fetch_source_partition_key(self, src_conn, schema: str, table: str) -> Optional[str]:
        rows = self._fetch(
            src_conn,
            """
            SELECT pg_get_partkeydef(c.oid)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND c.relkind = 'p'
            """,
            (schema, table),
        )
        return rows[0][0] if rows else None

    def _fetch_source_partition_bound(self, src_conn, schema: str, table: str) -> Optional[str]:
        rows = self._fetch(
            src_conn,
            """
            SELECT pg_get_expr(c.relpartbound, c.oid)
            FROM pg_class c
            JOIN pg_namespace n ON n.oid = c.relnamespace
            WHERE n.nspname = %s
              AND c.relname = %s
              AND c.relispartition
            """,
            (schema, table),
        )
        return rows[0][0] if rows else None

    def _ensure_sequences_for_defaults(self, tgt_conn, schema_fallback: str, default_sqls: List[str]) -> None:
        seq_qnames: List[str] = []
        for d in default_sqls:
            if not d:
                continue
            seq_qnames.extend(_NEXTVAL_RE.findall(d))
        if not seq_qnames:
            return

        with tgt_conn.cursor() as cur:
            for qname in sorted(set(seq_qnames)):
                qname = qname.replace('"', "")
                if "." in qname:
                    sch, seq = qname.split(".", 1)
                else:
                    sch, seq = schema_fallback, qname
                cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(sch)))
                cur.execute(sql.SQL("CREATE SEQUENCE IF NOT EXISTS {}.{}").format(sql.Identifier(sch), sql.Identifier(seq)))

    def ensure_table_like_source(self, schema: str, table: str) -> None:
        if not self.auto_ddl:
            return

        src = self._src()
        tgt = self._tgt()
        try:
            # run DDL in autocommit mode; set mode FIRST
            src.autocommit = True
            tgt.autocommit = True
            self.set_session_settings(src)
            self.set_session_settings(tgt)

            # schema
            if not self.schema_exists(tgt, schema):
                with tgt.cursor() as cur:
                    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))

            # table
            if self.table_exists(tgt, schema, table):
                return

            partition_parent = self._fetch_source_partition_parent(src, schema, table)
            if partition_parent:
                parent_schema, parent_table = partition_parent
                partition_bound = self._fetch_source_partition_bound(src, schema, table)
                if not partition_bound:
                    raise RuntimeError(f"Source partition {schema}.{table} is missing a partition bound")
                self.ensure_schema(parent_schema)
                self.ensure_table_like_source(parent_schema, parent_table)
                with tgt.cursor() as cur:
                    cur.execute(
                        sql.SQL("CREATE TABLE IF NOT EXISTS {} PARTITION OF {} {}").format(
                            _fq(schema, table),
                            _fq(parent_schema, parent_table),
                            sql.SQL(partition_bound),
                        )
                    )
                return

            cols = self._fetch_source_columns_for_ddl(src, schema, table)
            if not cols:
                raise RuntimeError(f"Source table {schema}.{table} has no columns")

            pk_cols = self._fetch_source_primary_key(src, schema, table)
            partition_key = self._fetch_source_partition_key(src, schema, table)

            # Create sequences referenced by DEFAULT nextval(...) before CREATE TABLE
            self._ensure_sequences_for_defaults(
                tgt, schema_fallback=schema, default_sqls=[c.get("default_sql") for c in cols]
            )

            col_defs = []
            for c in cols:
                parts = [sql.Identifier(c["name"]), sql.SQL(c["type_sql"])]

                # Identity columns: prefer identity over copying default nextval
                if c.get("attidentity") in ("a", "d"):
                    if c["attidentity"] == "a":
                        parts.append(sql.SQL("GENERATED ALWAYS AS IDENTITY"))
                    else:
                        parts.append(sql.SQL("GENERATED BY DEFAULT AS IDENTITY"))
                else:
                    if c.get("default_sql"):
                        parts.append(sql.SQL("DEFAULT "))
                        parts.append(sql.SQL(c["default_sql"]))

                if c.get("not_null"):
                    parts.append(sql.SQL("NOT NULL"))

                col_defs.append(sql.SQL(" ").join(parts))

            constraints = []
            if pk_cols:
                constraints.append(
                    sql.SQL("PRIMARY KEY ({})").format(sql.SQL(", ").join(sql.Identifier(x) for x in pk_cols))
                )

            if partition_key:
                create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({}) PARTITION BY {};").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(col_defs + constraints),
                    sql.SQL(partition_key),
                )
            else:
                create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
                    sql.Identifier(schema),
                    sql.Identifier(table),
                    sql.SQL(", ").join(col_defs + constraints),
                )

            with tgt.cursor() as cur:
                cur.execute(create_stmt)

        finally:
            self._put_src(src)
            self._put_tgt(tgt)
            
    def _copy_table_psycopg2(self, schema: str, table: str) -> None:
        if self.truncate_first and not self.allow_destructive:
            raise RuntimeError("Refusing to TRUNCATE without engine.allow_destructive=true")
    
        src = self._src()
        tgt = self._tgt()
        try:
            # Source can be autocommit; target transactional for TRUNCATE+COPY
            src.autocommit = True
            tgt.autocommit = False
            self.set_session_settings(src)
            self.set_session_settings(tgt)
    
            with tgt.cursor() as cur:
                if self.truncate_first:
                    cur.execute(sql.SQL("TRUNCATE TABLE {} CASCADE").format(_fq(schema, table)))
            
            copy_out = f'COPY (SELECT * FROM "{schema}"."{table}") TO STDOUT WITH (FORMAT CSV)'
            copy_in  = f'COPY "{schema}"."{table}" FROM STDIN WITH (FORMAT CSV)'
    
            with src.cursor() as src_cur, tgt.cursor() as tgt_cur:
                # ✅ binary temp file to handle bytes from psycopg2 COPY
                with tempfile.NamedTemporaryFile(
                    mode="w+b",
                    suffix=f"__{schema}__{table}.csv",
                    dir=self.spool_dir,
                ) as f:
                    src_cur.copy_expert(copy_out, f)
                    f.seek(0)
                    tgt_cur.copy_expert(copy_in, f)
    
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

    def _copy_table_spark_jdbc(self, schema: str, table: str, transfer: Dict[str, Any]) -> None:
        if transfer.get("geometry_mode") not in (None, "default") or transfer.get("has_geometry"):
            self._copy_table_psycopg2(schema, table)
            return

        try:
            from pyspark.sql import SparkSession
        except Exception as exc:
            raise RuntimeError(f"spark_jdbc engine requested but pyspark is not installed: {exc}") from exc

        src_conn = self._src()
        tgt_conn = self._tgt()
        try:
            src_conn.autocommit = True
            tgt_conn.autocommit = True

            chunk_column = transfer.get("chunk_column")
            lower_bound = upper_bound = None
            if chunk_column:
                with src_conn.cursor() as cur:
                    try:
                        cur.execute(
                            sql.SQL("SELECT MIN({}), MAX({}) FROM {}").format(
                                sql.Identifier(chunk_column),
                                sql.Identifier(chunk_column),
                                _fq(schema, table),
                            )
                        )
                        lower_bound, upper_bound = cur.fetchone()
                    except Exception:
                        chunk_column = None

            if self.truncate_first:
                if not self.allow_destructive:
                    raise RuntimeError("Refusing to TRUNCATE without engine.allow_destructive=true")
                with tgt_conn.cursor() as cur:
                    cur.execute(sql.SQL("TRUNCATE TABLE {} CASCADE").format(_fq(schema, table)))

            spark = SparkSession.builder.appName("AgenticDbMigrator").getOrCreate()
            jdbc_url_src = (
                f"jdbc:postgresql://{self.cfg['source']['host']}:{self.cfg['source'].get('port', 5432)}/{self.cfg['source']['database']}"
            )
            jdbc_url_tgt = (
                f"jdbc:postgresql://{self.cfg['target']['host']}:{self.cfg['target'].get('port', 5432)}/{self.cfg['target']['database']}"
            )
            read_options = {
                "url": jdbc_url_src,
                "dbtable": f'"{schema}"."{table}"',
                "user": self.cfg["source"]["user"],
                "password": self.cfg["source"]["password"],
                "driver": "org.postgresql.Driver",
                "fetchsize": "10000",
            }
            chunk_count = int(transfer.get("chunk_count") or 1)
            if chunk_column and lower_bound is not None and upper_bound is not None and lower_bound != upper_bound:
                read_options.update(
                    {
                        "partitionColumn": chunk_column,
                        "lowerBound": str(lower_bound),
                        "upperBound": str(upper_bound),
                        "numPartitions": str(chunk_count),
                    }
                )

            df = spark.read.format("jdbc").options(**read_options).load()
            if chunk_count > 1 and "numPartitions" not in read_options:
                df = df.repartition(chunk_count)

            write_options = {
                "url": jdbc_url_tgt,
                "dbtable": f'"{schema}"."{table}"',
                "user": self.cfg["target"]["user"],
                "password": self.cfg["target"]["password"],
                "driver": "org.postgresql.Driver",
                "batchsize": str(self.cfg.get("engine", {}).get("copy", {}).get("batchsize", 10000)),
            }
            df.write.format("jdbc").options(**write_options).mode("append").save()
        finally:
            self._put_src(src_conn)
            self._put_tgt(tgt_conn)
 
    def sync_sequences(self, schema: str, table: str) -> None:
        """
        Sync serial and identity-backed sequences to the current max value.
        """
        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)
    
            with tgt.cursor() as cur:
                cur.execute(
                    """
                    SELECT column_name, column_default, is_identity
                    FROM information_schema.columns
                    WHERE table_schema=%s
                      AND table_name=%s
                      AND (
                        (column_default IS NOT NULL AND column_default LIKE %s)
                        OR is_identity = 'YES'
                      )
                    """,
                    (schema, table, "nextval(%"),
                )
                rows = cur.fetchall()

            for col, default, is_identity in rows:
                seq_regclass: Optional[str] = None
                if is_identity == "YES":
                    with tgt.cursor() as cur:
                        cur.execute(
                            "SELECT pg_get_serial_sequence(%s, %s)",
                            (f'"{schema}"."{table}"', col),
                        )
                        seq_row = cur.fetchone()
                        seq_regclass = seq_row[0] if seq_row else None

                if not seq_regclass:
                    m = _NEXTVAL_RE.search(default or "")
                    if not m:
                        continue

                    qname = (m.group(1) or "").replace('"', "").strip()
                    if not qname:
                        continue

                    if "." in qname:
                        seq_schema, seq_name = qname.split(".", 1)
                    else:
                        seq_schema, seq_name = schema, qname
                    seq_regclass = f'"{seq_schema}"."{seq_name}"'
    
                with tgt.cursor() as cur:
                    cur.execute(
                        sql.SQL("SELECT COALESCE(MAX({}), 0) FROM {}").format(
                            sql.Identifier(col),
                            _fq(schema, table),
                        )
                    )
                    mx_row = cur.fetchone()
                    mx = int(mx_row[0] or 0) if mx_row else 0
    
                    if mx <= 0:
                        cur.execute("SELECT setval(%s::regclass, 1, false)", (seq_regclass,))
                    else:
                        cur.execute("SELECT setval(%s::regclass, %s, true)", (seq_regclass, mx))
        finally:
            self._put_tgt(tgt)

    

    def create_indexes(self, schema: str, table: str, indexes: List[Dict[str, Any]]) -> None:
        if not indexes:
            return

        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)

            for idx in indexes:
                stmt = idx.get("index_definition")
                cluster_stmt = idx.get("cluster_statement")
                if not stmt:
                    continue

                needs_autocommit = "CONCURRENTLY" in stmt.upper()
                prev_autocommit = tgt.autocommit
                if needs_autocommit:
                    tgt.autocommit = True

                try:
                    with tgt.cursor() as cur:
                        try:
                            cur.execute(stmt)
                        except psycopg2.Error as e:
                            if e.pgcode in ("42P07", "42710"):
                                pass
                            else:
                                raise
                        if cluster_stmt:
                            cur.execute(cluster_stmt)
                finally:
                    tgt.autocommit = prev_autocommit
        finally:
            self._put_tgt(tgt)

    def add_fks(self, schema: str, fks: List[Dict[str, Any]]) -> None:
        if not fks:
            return

        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)

            for fk in fks:
                t_schema = fk.get("schema", schema)
                t_table = fk.get("table")
                conname = fk.get("name")
                condef = fk.get("definition")
                if not (t_table and conname and condef):
                    continue

                stmt = sql.SQL("ALTER TABLE {} ADD CONSTRAINT {} {}").format(
                    _fq(t_schema, t_table),
                    sql.Identifier(conname),
                    sql.SQL(condef),
                )
                with tgt.cursor() as cur:
                    try:
                        cur.execute(stmt)
                    except psycopg2.Error as e:
                        if e.pgcode in ("42710",):
                            pass
                        else:
                            raise
        finally:
            self._put_tgt(tgt)

    def create_matviews(self, schema: str, matviews: List[Dict[str, Any]]) -> None:
        if not matviews:
            return

        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)

            if not self.schema_exists(tgt, schema):
                with tgt.cursor() as cur:
                    cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(schema)))

            for mv in matviews:
                name = mv.get("name")
                definition = mv.get("definition")
                if not name or not definition:
                    continue
                definition = definition.strip().rstrip(";")
                if self.matview_exists(tgt, schema, name):
                    continue
                strategy = mv.get("strategy", "direct_rebuild")
                staging_table = mv.get("staging_table")
                if strategy == "staged_rebuild" and staging_table:
                    stmt = sql.SQL("CREATE MATERIALIZED VIEW {} AS SELECT * FROM {} WITH DATA").format(
                        _fq(schema, name),
                        _fq(schema, staging_table),
                    )
                else:
                    stmt = sql.SQL("CREATE MATERIALIZED VIEW {} AS {} WITH DATA").format(
                        _fq(schema, name),
                        sql.SQL(definition),
                    )
                with tgt.cursor() as cur:
                    cur.execute(stmt)
        finally:
            self._put_tgt(tgt)

    def stage_matviews(self, schema: str, matviews: List[Dict[str, Any]]) -> None:
        if not matviews:
            return

        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)

            for mv in matviews:
                name = mv.get("name")
                definition = mv.get("definition")
                staging_table = mv.get("staging_table")
                if not (name and definition and staging_table):
                    continue
                definition = definition.strip().rstrip(";")
                with tgt.cursor() as cur:
                    cur.execute(sql.SQL("DROP TABLE IF EXISTS {} CASCADE").format(_fq(schema, staging_table)))
                    cur.execute(
                        sql.SQL("CREATE TABLE {} AS {}").format(
                            _fq(schema, staging_table),
                            sql.SQL(definition),
                        )
                    )
        finally:
            self._put_tgt(tgt)

    def create_mv_indexes(self, schema: str, indexes: List[Dict[str, Any]]) -> None:
        if not indexes:
            return

        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)

            for idx in indexes:
                stmt = idx.get("index_definition")
                cluster_stmt = idx.get("cluster_statement")
                if not stmt:
                    continue
                with tgt.cursor() as cur:
                    try:
                        cur.execute(stmt)
                    except psycopg2.Error as e:
                        if e.pgcode in ("42P07", "42710"):
                            pass
                        else:
                            raise
                    if cluster_stmt:
                        cur.execute(cluster_stmt)
        finally:
            self._put_tgt(tgt)

    def create_udfs(self, schema: str, udfs: List[Dict[str, Any]]) -> None:
        if not udfs:
            return
        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)

            for f in udfs:
                stmt = f.get("create_statement")
                if not stmt:
                    continue
                with tgt.cursor() as cur:
                    cur.execute(stmt)
        finally:
            self._put_tgt(tgt)

    def apply_grants(self, schema: str, grants: List[Dict[str, Any]]) -> None:
        if not grants:
            return

        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)
            with tgt.cursor() as cur:
                for grant in grants:
                    grantee = grant.get("grantee")
                    privilege = grant.get("privilege_type")
                    object_type = grant.get("object_type")
                    object_name = grant.get("object_name")
                    object_schema = grant.get("schema", schema)
                    if not grantee or not privilege:
                        continue
                    if object_type == "schema":
                        stmt = sql.SQL("GRANT {} ON SCHEMA {} TO {}").format(
                            sql.SQL(privilege),
                            sql.Identifier(object_schema),
                            sql.Identifier(grantee),
                        )
                    elif object_name:
                        stmt = sql.SQL("GRANT {} ON TABLE {} TO {}").format(
                            sql.SQL(privilege),
                            _fq(object_schema, object_name),
                            sql.Identifier(grantee),
                        )
                    else:
                        continue
                    try:
                        cur.execute(stmt)
                    except psycopg2.Error as exc:
                        if exc.pgcode in ("42701", "42704"):
                            continue
                        raise
        finally:
            self._put_tgt(tgt)

    def analyze_table(self, schema: str, table: str, maintenance: Optional[Dict[str, Any]] = None) -> None:
        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)
            with tgt.cursor() as cur:
                cur.execute(sql.SQL("ANALYZE {}").format(_fq(schema, table)))
                if (maintenance or {}).get("cluster_if_needed"):
                    cur.execute(sql.SQL("CLUSTER {}").format(_fq(schema, table)))
        finally:
            self._put_tgt(tgt)

    def vacuum_analyze_table(self, schema: str, table: str, maintenance: Optional[Dict[str, Any]] = None) -> None:
        tgt = self._tgt()
        try:
            tgt.autocommit = True
            self.set_session_settings(tgt)
            with tgt.cursor() as cur:
                cur.execute(sql.SQL("VACUUM ANALYZE {}").format(_fq(schema, table)))
                if (maintenance or {}).get("cluster_if_needed"):
                    cur.execute(sql.SQL("CLUSTER {}").format(_fq(schema, table)))
        finally:
            self._put_tgt(tgt)

    def verify_table(self, schema: str, table: str, validate: Dict[str, Any]) -> Dict[str, Any]:
        import hashlib

        sample_hash = bool((validate or {}).get("sample_hash", False))
        sample_rows = int((validate or {}).get("sample_rows", 50))

        src = self._src()
        tgt = self._tgt()
        try:
            src.autocommit = True
            tgt.autocommit = True

            def count_rows(conn) -> int:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT COUNT(*) FROM {}").format(_fq(schema, table)))
                    return int(cur.fetchone()[0])

            def sample_fingerprint(conn) -> str:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        SELECT kcu.column_name
                        FROM information_schema.table_constraints tc
                        JOIN information_schema.key_column_usage kcu
                          ON tc.constraint_name = kcu.constraint_name
                         AND tc.table_schema = kcu.table_schema
                        WHERE tc.constraint_type='PRIMARY KEY'
                          AND tc.table_schema=%s AND tc.table_name=%s
                        ORDER BY kcu.ordinal_position
                        LIMIT 1
                        """,
                        (schema, table),
                    )
                    row = cur.fetchone()
                    order_col = row[0] if row else None

                    if order_col:
                        q = sql.SQL("SELECT * FROM {} ORDER BY {} LIMIT %s").format(
                            _fq(schema, table), sql.Identifier(order_col)
                        )
                        cur.execute(q, (sample_rows,))
                    else:
                        q = sql.SQL("SELECT * FROM {} ORDER BY 1 LIMIT %s").format(_fq(schema, table))
                        cur.execute(q, (sample_rows,))
                    rows = cur.fetchall()

                h = hashlib.sha256()
                for r in rows:
                    h.update(repr(r).encode("utf-8"))
                return h.hexdigest()

            src_n = count_rows(src)
            tgt_n = count_rows(tgt)

            out = {"schema": schema, "table": table, "source_rows": src_n, "target_rows": tgt_n, "ok": (src_n == tgt_n)}
            if sample_hash:
                out["source_sample_hash"] = sample_fingerprint(src)
                out["target_sample_hash"] = sample_fingerprint(tgt)
                out["sample_hash_ok"] = (out["source_sample_hash"] == out["target_sample_hash"])
                out["ok"] = out["ok"] and out["sample_hash_ok"]

            if bool((validate or {}).get("partition_fidelity", False)):
                src_children = {
                    (row[0], row[1], row[2] or "")
                    for row in self._fetch(
                        src,
                        """
                        SELECT cn.nspname, child.relname, pg_get_expr(child.relpartbound, child.oid)
                        FROM pg_inherits i
                        JOIN pg_class parent ON parent.oid = i.inhparent
                        JOIN pg_namespace pn ON pn.oid = parent.relnamespace
                        JOIN pg_class child ON child.oid = i.inhrelid
                        JOIN pg_namespace cn ON cn.oid = child.relnamespace
                        WHERE pn.nspname = %s AND parent.relname = %s
                        ORDER BY cn.nspname, child.relname
                        """,
                        (schema, table),
                    )
                }
                tgt_children = {
                    (row[0], row[1], row[2] or "")
                    for row in self._fetch(
                        tgt,
                        """
                        SELECT cn.nspname, child.relname, pg_get_expr(child.relpartbound, child.oid)
                        FROM pg_inherits i
                        JOIN pg_class parent ON parent.oid = i.inhparent
                        JOIN pg_namespace pn ON pn.oid = parent.relnamespace
                        JOIN pg_class child ON child.oid = i.inhrelid
                        JOIN pg_namespace cn ON cn.oid = child.relnamespace
                        WHERE pn.nspname = %s AND parent.relname = %s
                        ORDER BY cn.nspname, child.relname
                        """,
                        (schema, table),
                    )
                }
                out["partition_children_ok"] = src_children == tgt_children
                out["ok"] = out["ok"] and out["partition_children_ok"]
            return out
        finally:
            self._put_src(src)
            self._put_tgt(tgt)


def execute(
    cfg: Dict[str, Any],
    plan_path: Optional[str] = None,
    state_path: str = "state.json",
    plan_obj: Optional[Dict[str, Any]] = None,
) -> None:
    orch = MigrationOrchestrator(cfg)
    state_file = Path(state_path)

    state = _read_json(state_file) if state_file.exists() else {"completed": {}, "started_at": time.time()}
    completed = state.get("completed", {})

    def mark(step_id: str, payload: Dict[str, Any]) -> None:
        completed[step_id] = payload
        state["completed"] = completed
        state["updated_at"] = time.time()
        _write_json(state_file, state)

    try:
        if plan_obj is None and not plan_path:
            raise RuntimeError("V2 executor expects a plan.json (plan-driven).")

        plan = plan_obj if plan_obj is not None else _read_json(plan_path)
        steps = plan.get("steps", [])
        if not steps:
            raise RuntimeError("plan.json has no steps.")

        for step in steps:
            step_id = step.get("id") or f"{step.get('schema')}.{step.get('table')}.{step.get('op')}"
            if step_id in completed:
                print(f"↩️  Skipping {step_id}")
                continue

            op = step.get("op")
            t0 = time.time()
            print(f"➡️  {op} ... {step.get('schema','')}.{step.get('table','')}".strip())

            try:
                if op == "ensure_schema":
                    orch.ensure_schema(step["schema"])

                elif op == "create_udfs":
                    orch.create_udfs(step["schema"], step.get("udfs", []))

                elif op == "ensure_table":
                    orch.ensure_table_like_source(step["schema"], step["table"])

                elif op == "copy_table":
                    orch.copy_table(step["schema"], step["table"], transfer=step.get("transfer", {}) or {})

                elif op == "sync_sequences":
                    orch.sync_sequences(step["schema"], step["table"])

                elif op == "create_indexes":
                    orch.create_indexes(step["schema"], step["table"], step.get("indexes", []))

                elif op == "add_fks":
                    orch.add_fks(step["schema"], step.get("fks", []))

                elif op == "apply_grants":
                    orch.apply_grants(step["schema"], step.get("grants", []))

                elif op == "stage_matviews":
                    orch.stage_matviews(step["schema"], step.get("matviews", []))

                elif op == "create_matviews":
                    orch.create_matviews(step["schema"], step.get("matviews", []))

                elif op == "create_mv_indexes":
                    orch.create_mv_indexes(step["schema"], step.get("indexes", []))

                elif op == "analyze_table":
                    orch.analyze_table(step["schema"], step["table"], maintenance=step.get("maintenance", {}) or {})

                elif op == "vacuum_analyze_table":
                    orch.vacuum_analyze_table(step["schema"], step["table"], maintenance=step.get("maintenance", {}) or {})

                elif op == "verify_table":
                    rep = orch.verify_table(step["schema"], step["table"], step.get("validate", {}) or {})
                    if not rep.get("ok", False):
                        raise RuntimeError(f"Verification failed: {rep}")
                    mark(step_id, {"ok": True, "elapsed_s": round(time.time() - t0, 3), "verify": rep})
                    print(f"✅ {op} OK in {round(time.time()-t0,3)}s")
                    continue

                else:
                    raise RuntimeError(f"Unknown op: {op}")

                mark(step_id, {"ok": True, "elapsed_s": round(time.time() - t0, 3)})
                print(f"✅ {op} OK in {round(time.time()-t0,3)}s")

            except Exception as e:
                mark(step_id, {"ok": False, "elapsed_s": round(time.time() - t0, 3), "error": str(e)})
                print(f"❌ {op} failed: {e}")
                raise

    finally:
        orch.close()
