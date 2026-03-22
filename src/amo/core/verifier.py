from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import psycopg2


def _dsn(db_cfg: Dict[str, Any]) -> str:
    return (
        f"host={db_cfg['host']} port={db_cfg.get('port', 5432)} dbname={db_cfg['database']} "
        f"user={db_cfg['user']} password={db_cfg['password']}"
    )


def _fq(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def _read_json(path: str | Path) -> Dict[str, Any]:
    return json.loads(Path(path).read_text())


def write_report(report: Dict[str, Any], out_path: str | Path) -> None:
    Path(out_path).write_text(json.dumps(report, indent=2, sort_keys=True))


def _count_rows(conn, schema: str, table: str) -> int:
    with conn.cursor() as cur:
        cur.execute(f"SELECT COUNT(*) FROM {_fq(schema, table)};")
        return int(cur.fetchone()[0])


def _sample_hash(conn, schema: str, table: str, sample_rows: int = 50) -> str:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
              AND tc.table_schema = %s
              AND tc.table_name = %s
            ORDER BY kcu.ordinal_position
            LIMIT 1
            """,
            (schema, table),
        )
        row = cur.fetchone()
        order_col = row[0] if row else None

        if order_col:
            q = f'SELECT * FROM {_fq(schema, table)} ORDER BY "{order_col}" LIMIT %s'
        else:
            q = f"SELECT * FROM {_fq(schema, table)} ORDER BY 1 LIMIT %s"

        cur.execute(q, (sample_rows,))
        rows = cur.fetchall()

    h = hashlib.sha256()
    for r in rows:
        h.update(repr(r).encode("utf-8"))
    return h.hexdigest()


def verify_plan(cfg: Dict[str, Any], plan_path: str) -> Dict[str, Any]:
    plan = _read_json(plan_path)
    steps: List[Dict[str, Any]] = plan.get("steps", [])
    if not steps:
        raise RuntimeError("plan.json has no steps to verify.")

    src_dsn = _dsn(cfg["source"])
    tgt_dsn = _dsn(cfg["target"])

    results = []
    ok_all = True
    verify_steps = [step for step in steps if step.get("op") == "verify_table"]
    candidate_steps = verify_steps or [step for step in steps if step.get("op") == "copy_table"]

    with psycopg2.connect(src_dsn) as src_conn, psycopg2.connect(tgt_dsn) as tgt_conn:
        src_conn.autocommit = True
        tgt_conn.autocommit = True

        for step in candidate_steps:
            schema = step["schema"]
            table = step["table"]
            validate = step.get("validate", {}) or {}

            src_rows = _count_rows(src_conn, schema, table)
            tgt_rows = _count_rows(tgt_conn, schema, table)
            ok = (src_rows == tgt_rows)

            row_obj: Dict[str, Any] = {
                "schema": schema,
                "table": table,
                "source_rows": src_rows,
                "target_rows": tgt_rows,
                "ok": ok,
            }

            if bool(validate.get("sample_hash", False)):
                sample_rows = int(validate.get("sample_rows", 50))
                row_obj["source_sample_hash"] = _sample_hash(src_conn, schema, table, sample_rows=sample_rows)
                row_obj["target_sample_hash"] = _sample_hash(tgt_conn, schema, table, sample_rows=sample_rows)
                row_obj["sample_hash_ok"] = (row_obj["source_sample_hash"] == row_obj["target_sample_hash"])
                ok = ok and row_obj["sample_hash_ok"]
                row_obj["ok"] = ok

            if not ok:
                ok_all = False

            results.append(row_obj)

    return {
        "ok": ok_all,
        "tables_checked": len(results),
        "results": results,
    }
