from __future__ import annotations

from io import StringIO
from typing import Any, Dict, Optional

import psycopg2


def dsn_from_cfg(db_cfg: Dict[str, Any]) -> str:
    return (
        f"host={db_cfg['host']} port={db_cfg.get('port', 5432)} dbname={db_cfg['database']} "
        f"user={db_cfg['user']} password={db_cfg['password']}"
    )


def _fq(schema: str, table: str) -> str:
    return f'"{schema}"."{table}"'


def copy_table_copy(
    cfg: Dict[str, Any],
    schema: str,
    table: str,
    truncate_first: bool = True,
) -> Dict[str, Any]:
    """
    Portable demo path:
      - target table must already exist (for demo seed, you’ll create it in source.sql for target too OR create during run)
      - COPY OUT from source -> COPY IN to target via STDOUT/STDIN streaming

    Returns basic stats for logging/reporting.
    """
    src_dsn = dsn_from_cfg(cfg["source"])
    tgt_dsn = dsn_from_cfg(cfg["target"])

    # You can add include/exclude columns later; for now use all columns
    select_sql = f"COPY (SELECT * FROM {_fq(schema, table)}) TO STDOUT WITH CSV HEADER"
    insert_sql = f"COPY {_fq(schema, table)} FROM STDIN WITH CSV HEADER"

    transferred_bytes = 0

    with psycopg2.connect(src_dsn) as src_conn, psycopg2.connect(tgt_dsn) as tgt_conn:
        src_conn.autocommit = True
        tgt_conn.autocommit = True

        with src_conn.cursor() as src_cur, tgt_conn.cursor() as tgt_cur:
            if truncate_first:
                tgt_cur.execute(f"TRUNCATE TABLE {_fq(schema, table)};")

            # Stream COPY through an in-memory buffer (simple + portable)
            buf = StringIO()
            src_cur.copy_expert(select_sql, buf)
            data = buf.getvalue()
            transferred_bytes = len(data.encode("utf-8"))

            buf2 = StringIO(data)
            tgt_cur.copy_expert(insert_sql, buf2)

    return {
        "schema": schema,
        "table": table,
        "bytes": transferred_bytes,
        "engine": "copy",
    }
