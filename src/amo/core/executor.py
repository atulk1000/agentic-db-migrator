from __future__ import annotations

import re
import json
import time
from pathlib import Path
from typing import Any, Dict, Optional, List

import psycopg2
from psycopg2 import sql


# -----------------------------
# Helpers
# -----------------------------

def _dsn(db_cfg: Dict[str, Any]) -> str:
    return (
        f"host={db_cfg['host']} port={db_cfg['port']} dbname={db_cfg['database']} "
        f"user={db_cfg['user']} password={db_cfg['password']}"
    )

def _load_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text())

def _write_json(path: str | Path, obj: Dict[str, Any]) -> None:
    Path(path).write_text(json.dumps(obj, indent=2, sort_keys=True))

def _fq(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'

def _schema_exists(conn, schema: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            "SELECT 1 FROM information_schema.schemata WHERE schema_name = %s",
            (schema,),
        )
        return cur.fetchone() is not None

def _table_exists(conn, schema: str, table: str) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema = %s AND table_name = %s
            """,
            (schema, table),
        )
        return cur.fetchone() is not None

def _count_rows(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {_fq(schema, table)};")
        return int(cur.fetchone()[0])

_NEXTVAL_RE = re.compile(r"nextval\('([^']+)'\s*(?:::regclass)?\)", re.IGNORECASE)

def _extract_nextval_seq_qnames(default_sql: Optional[str]) -> List[str]:
    """
    Returns list of sequence qualified names referenced by nextval('...').
    Works for: nextval('schema.seq'::regclass) and nextval('schema.seq')
    """
    if not default_sql:
        return []
    return _NEXTVAL_RE.findall(default_sql)

def _split_qname(qname: str, fallback_schema: str) -> tuple[str, str]:
    """
    Split schema-qualified name like analytics.events_event_id_seq.
    If unqualified, assume fallback_schema.
    Strips any double-quotes if present.
    """
    qname = qname.replace('"', "")
    if "." in qname:
        sch, name = qname.split(".", 1)
        return sch, name
    return fallback_schema, qname

def _ensure_target_sequences_for_create(tgt_conn, seq_qnames: List[str], fallback_schema: str) -> None:
    """
    Creates any sequences referenced by nextval(...) defaults.
    Must run before CREATE TABLE that references them.
    """
    if not seq_qnames:
        return

    with tgt_conn.cursor() as cur:
        for qname in sorted(set(seq_qnames)):
            sch, seq = _split_qname(qname, fallback_schema=fallback_schema)
            # ensure schema exists for the sequence
            cur.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(sql.Identifier(sch)))
            cur.execute(
                sql.SQL("CREATE SEQUENCE IF NOT EXISTS {}.{}").format(
                    sql.Identifier(sch), sql.Identifier(seq)
                )
            )


# -----------------------------
# Auto-DDL from source
# -----------------------------

def _fetch_source_columns(conn, schema: str, table: str):
    """
    Returns list of dicts:
      {name, type_sql, not_null, default_sql}
    type_sql uses pg_catalog.format_type so it’s accurate.
    default_sql is pg_get_expr(adbin, adrelid) when present.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                a.attname AS col_name,
                pg_catalog.format_type(a.atttypid, a.atttypmod) AS type_sql,
                a.attnotnull AS not_null,
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
        rows = cur.fetchall()

    cols = []
    for col_name, type_sql, not_null, default_sql in rows:
        cols.append(
            {
                "name": col_name,
                "type_sql": type_sql,
                "not_null": bool(not_null),
                "default_sql": default_sql,  # can be None
            }
        )
    return cols

def _fetch_source_primary_key(conn, schema: str, table: str):
    """
    Returns list of PK columns in order, or [] if none.
    """
    with conn.cursor() as cur:
        cur.execute(
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
        return [r[0] for r in cur.fetchall()]

def _ensure_target_schema_and_table_like_source(
    src_conn,
    tgt_conn,
    schema: str,
    table: str,
) -> None:
    ...
    cols = _fetch_source_columns(src_conn, schema, table)
    if not cols:
        raise RuntimeError(f"Source table {schema}.{table} has no columns (unexpected).")

    pk_cols = _fetch_source_primary_key(src_conn, schema, table)

    col_defs_sql = []
    seq_qnames: List[str] = []

    for c in cols:
        parts = [sql.Identifier(c["name"]), sql.SQL(c["type_sql"])]

        if c["default_sql"]:
            seq_qnames.extend(_extract_nextval_seq_qnames(c["default_sql"]))
            parts.append(sql.SQL("DEFAULT "))
            parts.append(sql.SQL(c["default_sql"]))

        if c["not_null"]:
            parts.append(sql.SQL("NOT NULL"))

        col_defs_sql.append(sql.SQL(" ").join(parts))

    all_defs = col_defs_sql + constraints_sql
    
    ...
    create_stmt = sql.SQL("CREATE TABLE IF NOT EXISTS {}.{} ({});").format(
        sql.Identifier(schema),
        sql.Identifier(table),
        sql.SQL(", ").join(all_defs),
    )

    with tgt_conn.cursor() as cur:
        # Create any referenced sequences BEFORE running CREATE TABLE
        _ensure_target_sequences_for_create(tgt_conn, seq_qnames, fallback_schema=schema)
        cur.execute(create_stmt)


# -----------------------------
# COPY pipeline
# -----------------------------

def _copy_table_csv(
    src_conn,
    tgt_conn,
    schema: str,
    table: str,
    truncate_first: bool = True,
    statement_timeout_ms: Optional[int] = None,
    auto_ddl: bool = False,
) -> None:
    """
    Portable COPY pipeline:
      src: COPY (SELECT * FROM schema.table) TO STDOUT WITH CSV
      tgt: (optional TRUNCATE) + COPY schema.table FROM STDIN WITH CSV
    """
    if statement_timeout_ms:
        with src_conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {int(statement_timeout_ms)};")
        with tgt_conn.cursor() as cur:
            cur.execute(f"SET statement_timeout = {int(statement_timeout_ms)};")

    if not _table_exists(tgt_conn, schema, table):
        if auto_ddl:
            _ensure_target_schema_and_table_like_source(
                src_conn=src_conn,
                tgt_conn=tgt_conn,
                schema=schema,
                table=table,
            )
            if not _table_exists(tgt_conn, schema, table):
                raise RuntimeError(
                    f"Auto-DDL ran but target table {_fq(schema, table)} still does not exist."
                )
        else:
            raise RuntimeError(
                f"Target table {_fq(schema, table)} does not exist. "
                f"Enable engine.auto_ddl=true (auto-create from source) or seed the target with DDL."
            )

    with tgt_conn.cursor() as tgt_cur:
        if truncate_first:
            tgt_cur.execute(f"TRUNCATE TABLE {_fq(schema, table)};")

        with src_conn.cursor() as src_cur:
            copy_out = f'COPY (SELECT * FROM {_fq(schema, table)}) TO STDOUT WITH (FORMAT CSV, HEADER false)'
            copy_in  = f'COPY {_fq(schema, table)} FROM STDIN WITH (FORMAT CSV, HEADER false)'

            import io
            buf = io.StringIO()
            src_cur.copy_expert(copy_out, buf)
            buf.seek(0)
            tgt_cur.copy_expert(copy_in, buf)


# -----------------------------
# Executor (V1)
# -----------------------------

def execute(cfg: Dict[str, Any], plan_path: str, state_path: str) -> None:
    plan = _load_json(plan_path)

    state_file = Path(state_path)
    if state_file.exists():
        state = _load_json(state_file)
    else:
        state = {"completed_steps": {}, "started_at": time.time(), "plan_path": str(plan_path)}

    completed = state.get("completed_steps", {})

    src_cfg = cfg["source"]
    tgt_cfg = cfg["target"]

    # Optional knobs
    truncate_first = bool(cfg.get("engine", {}).get("copy", {}).get("truncate_first", True))
    statement_timeout_ms = cfg.get("engine", {}).get("copy", {}).get("statement_timeout_ms")
    auto_ddl = bool(cfg.get("engine", {}).get("auto_ddl", False))

    src_dsn = _dsn(src_cfg)
    tgt_dsn = _dsn(tgt_cfg)

    steps: List[Dict[str, Any]] = plan.get("steps", [])
    if not steps:
        raise RuntimeError("plan.json has no steps. Did your planner write steps?")

    with psycopg2.connect(src_dsn) as src_conn, psycopg2.connect(tgt_dsn) as tgt_conn:
        src_conn.autocommit = True
        tgt_conn.autocommit = False  # transactional writes

        for step in steps:
            step_id = step.get("id") or f"{step.get('schema')}.{step.get('table')}"
            if step_id in completed:
                print(f"↩️  Skipping {step_id} (already completed)")
                continue

            op = step.get("op")
            if op != "copy_table":
                raise RuntimeError(f"Unsupported op in v1 executor: {op}")

            schema = step["schema"]
            table = step["table"]

            print(f"➡️  Copying {schema}.{table} ...")
            t0 = time.time()

            try:
                _copy_table_csv(
                    src_conn=src_conn,
                    tgt_conn=tgt_conn,
                    schema=schema,
                    table=table,
                    truncate_first=truncate_first,
                    statement_timeout_ms=statement_timeout_ms,
                    auto_ddl=auto_ddl,
                )
                tgt_conn.commit()

                src_n = _count_rows(src_conn, schema, table)
                tgt_n = _count_rows(tgt_conn, schema, table)

                completed[step_id] = {
                    "schema": schema,
                    "table": table,
                    "src_rows": src_n,
                    "tgt_rows": tgt_n,
                    "ok": (src_n == tgt_n),
                    "elapsed_s": round(time.time() - t0, 3),
                }

                state["completed_steps"] = completed
                state["updated_at"] = time.time()
                _write_json(state_file, state)

                print(f"✅ {schema}.{table} done (src={src_n}, tgt={tgt_n}) in {completed[step_id]['elapsed_s']}s")

            except Exception as e:
                tgt_conn.rollback()
                print(f"❌ Failed {schema}.{table}: {e}")
                state["last_error"] = {"step_id": step_id, "error": str(e)}
                state["updated_at"] = time.time()
                _write_json(state_file, state)
                raise
