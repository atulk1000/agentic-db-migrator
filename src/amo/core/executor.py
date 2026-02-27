from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Tuple, List

import psycopg2


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

def _copy_table_csv(
    src_conn,
    tgt_conn,
    schema: str,
    table: str,
    truncate_first: bool = True,
    statement_timeout_ms: Optional[int] = None,
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
        raise RuntimeError(
            f"Target table {_fq(schema, table)} does not exist. "
            f"Seed the target with DDL (recommended) or implement auto-DDL creation."
        )

    with tgt_conn.cursor() as tgt_cur:
        if truncate_first:
            tgt_cur.execute(f"TRUNCATE TABLE {_fq(schema, table)};")

        with src_conn.cursor() as src_cur:
            copy_out = f'COPY (SELECT * FROM {_fq(schema, table)}) TO STDOUT WITH (FORMAT CSV, HEADER false)'
            copy_in  = f'COPY {_fq(schema, table)} FROM STDIN WITH (FORMAT CSV, HEADER false)'

            # stream rows: psycopg2 supports file-like objects; simplest is using copy_expert with STDOUT/STDIN
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
