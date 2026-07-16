from __future__ import annotations

import json
import os
from pathlib import Path

import psycopg2
import pytest

from amo.core.analysis import (
    build_approval_document,
    build_database_manifest,
    build_pre_migration_summary,
    diff_manifests,
    write_json,
)
from amo.core.executor import execute
from amo.core.planners.heuristic_planner import generate_plan
from amo.core.policy import ApprovalPolicyError, build_approved_execution_bundle

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("AMO_RUN_INTEGRATION") != "1",
        reason="set AMO_RUN_INTEGRATION=1 with isolated source and target PostgreSQL instances",
    ),
]


def _database(prefix: str, *, default_port: int, default_user: str) -> dict[str, object]:
    upper = prefix.upper()
    return {
        "host": os.environ.get(f"AMO_{upper}_HOST", "127.0.0.1"),
        "port": int(os.environ.get(f"AMO_{upper}_PORT", str(default_port))),
        "database": os.environ.get(
            f"AMO_{upper}_DB", "sourcedb" if prefix == "source" else "targetdb"
        ),
        "user": os.environ.get(f"AMO_{upper}_USER", default_user),
        "password": os.environ.get(f"AMO_{upper}_PASSWORD", default_user),
    }


def _config() -> dict:
    return {
        "engine": {
            "type": "copy",
            "auto_ddl": True,
            "allow_destructive": False,
            "verify_inline": False,
            "copy": {"truncate_first": False, "batchsize": 1000},
        },
        "source": _database("source", default_port=5433, default_user="source"),
        "target": _database("target", default_port=5434, default_user="target"),
        "migration": {
            "include_schemas": ["app", "audit"],
            "exclude_schemas": ["pg_catalog", "information_schema"],
            "exclude_tables": [],
            "exclude_suffixes": [],
            "include_udfs": True,
        },
    }


def _connect(db: dict[str, object]):
    return psycopg2.connect(**db)


def _execute_sql(db: dict[str, object], statement: str) -> None:
    with _connect(db) as conn:
        conn.autocommit = True
        with conn.cursor() as cursor:
            cursor.execute(statement)


def _fetch_all(db: dict[str, object], statement: str) -> list[tuple]:
    with _connect(db) as conn, conn.cursor() as cursor:
        cursor.execute(statement)
        return cursor.fetchall()


def _seed_source(cfg: dict) -> None:
    _execute_sql(
        cfg["source"],
        """
        DROP SCHEMA IF EXISTS app CASCADE;
        DROP SCHEMA IF EXISTS audit CASCADE;
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_reader') THEN
                CREATE ROLE app_reader NOLOGIN;
            END IF;
        END $$;
        CREATE SCHEMA app;
        CREATE SCHEMA audit;

        CREATE OR REPLACE FUNCTION app.normalize_name(value text)
        RETURNS text
        LANGUAGE sql
        IMMUTABLE
        AS $$ SELECT lower(trim(value)) $$;

        CREATE TABLE app.accounts (
            id serial PRIMARY KEY,
            name text NOT NULL,
            updated_at timestamp without time zone NOT NULL
        );
        CREATE INDEX idx_accounts_name ON app.accounts (name);
        INSERT INTO app.accounts (name, updated_at) VALUES
            ('Alice', '2026-07-01 10:00:00'),
            ('Bob', '2026-07-02 10:00:00');

        CREATE TABLE app.events (
            event_id bigserial PRIMARY KEY,
            payload text NOT NULL
        );
        INSERT INTO app.events (payload) VALUES ('created'), ('updated');

        CREATE TABLE app.orders (
            order_id serial PRIMARY KEY,
            account_id integer NOT NULL REFERENCES app.accounts(id),
            amount numeric(12, 2) NOT NULL
        );
        INSERT INTO app.orders (account_id, amount) VALUES (1, 10.00), (2, 25.00);

        CREATE TABLE app.order_items (
            item_id serial PRIMARY KEY,
            order_id integer NOT NULL,
            sku text NOT NULL,
            CONSTRAINT fk_order_items_order
                FOREIGN KEY (order_id) REFERENCES app.orders(order_id)
        );
        INSERT INTO app.order_items (order_id, sku) VALUES (1, 'sku-1'), (2, 'sku-2');

        CREATE TABLE audit.measurements (
            measurement_id bigint NOT NULL,
            measured_on date NOT NULL,
            reading numeric(10, 2) NOT NULL,
            PRIMARY KEY (measurement_id, measured_on)
        ) PARTITION BY RANGE (measured_on);
        CREATE TABLE audit.measurements_h1 PARTITION OF audit.measurements
            FOR VALUES FROM ('2026-01-01') TO ('2026-07-01');
        CREATE TABLE audit.measurements_h2 PARTITION OF audit.measurements
            FOR VALUES FROM ('2026-07-01') TO ('2027-01-01');
        INSERT INTO audit.measurements VALUES
            (1, '2026-03-01', 10.50),
            (2, '2026-09-01', 11.75);

        CREATE TABLE app.refreshable (id integer PRIMARY KEY, value text NOT NULL);
        INSERT INTO app.refreshable VALUES (1, 'source-one'), (2, 'source-two');

        CREATE TABLE app.retry_demo (id integer PRIMARY KEY, value text NOT NULL);
        INSERT INTO app.retry_demo VALUES (1, 'source-retry');

        CREATE MATERIALIZED VIEW app.account_count AS
            SELECT count(*)::bigint AS row_count FROM app.accounts;
        CREATE INDEX idx_account_count_rows ON app.account_count (row_count);

        GRANT USAGE ON SCHEMA app TO app_reader;
        GRANT SELECT ON app.accounts TO app_reader;
        GRANT SELECT ON app.account_count TO app_reader;
        ANALYZE;
        """,
    )


def _seed_target(cfg: dict) -> None:
    _execute_sql(
        cfg["target"],
        """
        DROP SCHEMA IF EXISTS app CASCADE;
        DROP SCHEMA IF EXISTS audit CASCADE;
        DO $$ BEGIN
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_reader') THEN
                CREATE ROLE app_reader NOLOGIN;
            END IF;
            IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'source') THEN
                CREATE ROLE source NOLOGIN;
            END IF;
        END $$;
        CREATE SCHEMA app;
        CREATE SCHEMA audit;

        CREATE TABLE app.accounts (
            id serial PRIMARY KEY,
            name text NOT NULL,
            updated_at timestamp without time zone NOT NULL
        );
        CREATE INDEX idx_accounts_name ON app.accounts (name);
        INSERT INTO app.accounts (id, name, updated_at)
            VALUES (1, 'stale-alice', '2025-01-01 00:00:00');

        CREATE TABLE app.events (
            event_id bigserial PRIMARY KEY,
            payload text NOT NULL
        );

        CREATE TABLE app.refreshable (id integer PRIMARY KEY, value text NOT NULL);
        INSERT INTO app.refreshable VALUES (999, 'target-only');

        CREATE TABLE app.retry_demo (id integer PRIMARY KEY, value text NOT NULL);
        INSERT INTO app.retry_demo VALUES (1, 'target-conflict');
        ANALYZE;
        """,
    )


def _build_artifacts(cfg: dict, root: Path) -> dict[str, Path]:
    paths = {
        "source": root / "source_manifest.json",
        "target": root / "target_manifest.json",
        "diff": root / "manifest_diff.json",
        "plan": root / "plan.json",
        "summary": root / "pre_migration_summary.json",
    }
    source = build_database_manifest(cfg, "source")
    target = build_database_manifest(cfg, "target")
    write_json(paths["source"], source)
    write_json(paths["target"], target)
    diff = diff_manifests(source, target)
    write_json(paths["diff"], diff)
    plan = generate_plan(str(paths["source"]))
    write_json(paths["plan"], plan)
    summary = build_pre_migration_summary(
        source_manifest=source,
        target_manifest=target,
        manifest_diff=diff,
        plan=plan,
        migration_mode="safe_sync",
    )
    write_json(paths["summary"], summary)
    return paths


def _approve(
    paths: dict[str, Path],
    root: Path,
    name: str,
    *,
    included: list[str],
    strategies: dict[str, dict],
    allow_destructive: bool = False,
) -> Path:
    approval_path = root / f"{name}_approval.json"
    approval = build_approval_document(
        plan_path=paths["plan"],
        summary_path=paths["summary"],
        source_manifest_path=paths["source"],
        approved_mode="safe_sync",
        approved_by="integration-test",
        allow_destructive=allow_destructive,
        include_tables=included,
        table_strategies=strategies,
    )
    write_json(approval_path, approval)
    return approval_path


def test_approved_postgres_workflow_end_to_end(tmp_path):
    cfg = _config()
    _seed_source(cfg)
    _seed_target(cfg)
    paths = _build_artifacts(cfg, tmp_path)

    base_tables = [
        "app.accounts",
        "app.events",
        "app.orders",
        "app.order_items",
        "audit.measurements",
    ]
    base_approval = _approve(
        paths,
        tmp_path,
        "base",
        included=base_tables,
        strategies={
            "app.accounts": {"strategy": "upsert", "conflict_key": ["id"]},
            "app.events": {"strategy": "append_only"},
            "app.orders": {"strategy": "append_only"},
            "app.order_items": {"strategy": "append_only"},
            "audit.measurements": {"strategy": "append_only"},
        },
    )
    base_bundle = build_approved_execution_bundle(approval_path=base_approval)
    execute(cfg=cfg, bundle=base_bundle, state_path=str(tmp_path / "base_state.json"))

    assert _fetch_all(cfg["target"], "SELECT id, name FROM app.accounts ORDER BY id") == [
        (1, "Alice"),
        (2, "Bob"),
    ]
    assert _fetch_all(cfg["target"], "SELECT count(*) FROM app.events") == [(2,)]
    assert _fetch_all(cfg["target"], "SELECT count(*) FROM app.order_items") == [(2,)]
    assert _fetch_all(cfg["target"], "SELECT count(*) FROM audit.measurements") == [(2,)]
    assert _fetch_all(cfg["target"], "SELECT app.normalize_name('  MiXeD  ')") == [("mixed",)]
    assert _fetch_all(cfg["target"], "SELECT row_count FROM app.account_count") == [(2,)]
    assert _fetch_all(
        cfg["target"],
        "SELECT count(*) FROM pg_indexes WHERE schemaname='app' AND indexname='idx_accounts_name'",
    ) == [(1,)]
    assert _fetch_all(
        cfg["target"],
        "SELECT count(*) FROM pg_constraint WHERE conname='fk_order_items_order'",
    ) == [(1,)]
    assert _fetch_all(
        cfg["target"],
        "SELECT has_schema_privilege('app_reader', 'app', 'USAGE'), "
        "has_table_privilege('app_reader', 'app.accounts', 'SELECT')",
    ) == [(True, True)]
    assert _fetch_all(cfg["target"], "SELECT last_value FROM app.accounts_id_seq") == [(2,)]

    refresh_before = _fetch_all(cfg["target"], "SELECT id, value FROM app.refreshable ORDER BY id")
    denied_approval = _approve(
        paths,
        tmp_path,
        "destructive_denied",
        included=["app.refreshable"],
        strategies={"app.refreshable": {"strategy": "truncate_reload"}},
    )
    with pytest.raises(ApprovalPolicyError, match="truncate_reload requires"):
        build_approved_execution_bundle(approval_path=denied_approval)
    assert (
        _fetch_all(cfg["target"], "SELECT id, value FROM app.refreshable ORDER BY id")
        == refresh_before
    )

    destructive_approval = _approve(
        paths,
        tmp_path,
        "destructive_allowed",
        included=["app.refreshable"],
        strategies={"app.refreshable": {"strategy": "truncate_reload"}},
        allow_destructive=True,
    )
    destructive_bundle = build_approved_execution_bundle(approval_path=destructive_approval)
    execute(
        cfg=cfg,
        bundle=destructive_bundle,
        state_path=str(tmp_path / "destructive_state.json"),
    )
    expected_refresh = [(1, "source-one"), (2, "source-two")]
    assert (
        _fetch_all(cfg["target"], "SELECT id, value FROM app.refreshable ORDER BY id")
        == expected_refresh
    )

    tampered_plan_path = tmp_path / "tampered_plan.json"
    tampered_plan_path.write_bytes(paths["plan"].read_bytes())
    tamper_paths = {**paths, "plan": tampered_plan_path}
    tampered_approval = _approve(
        tamper_paths,
        tmp_path,
        "tampered",
        included=["app.refreshable"],
        strategies={"app.refreshable": {"strategy": "append_only"}},
    )
    with tampered_plan_path.open("ab") as handle:
        handle.write(b"\n")
    with pytest.raises(ApprovalPolicyError, match="SHA-256 does not match"):
        build_approved_execution_bundle(approval_path=tampered_approval)
    assert (
        _fetch_all(cfg["target"], "SELECT id, value FROM app.refreshable ORDER BY id")
        == expected_refresh
    )

    untrusted_plan = json.loads(paths["plan"].read_text(encoding="utf-8"))
    udf_step = next(step for step in untrusted_plan["steps"] if step["op"] == "create_udfs")
    udf_step["udfs"][0] = {
        "schema": "app",
        "name": "not_in_manifest",
        "create_statement": "CREATE FUNCTION app.not_in_manifest() RETURNS int AS 'SELECT 1' LANGUAGE SQL",
    }
    untrusted_plan_path = tmp_path / "untrusted_plan.json"
    write_json(untrusted_plan_path, untrusted_plan)
    untrusted_paths = {**paths, "plan": untrusted_plan_path}
    untrusted_approval = _approve(
        untrusted_paths,
        tmp_path,
        "untrusted_metadata",
        included=["app.refreshable"],
        strategies={"app.refreshable": {"strategy": "append_only"}},
    )
    with pytest.raises(ApprovalPolicyError, match="unknown source UDF"):
        build_approved_execution_bundle(approval_path=untrusted_approval)
    assert (
        _fetch_all(cfg["target"], "SELECT id, value FROM app.refreshable ORDER BY id")
        == expected_refresh
    )

    retry_approval = _approve(
        paths,
        tmp_path,
        "retry",
        included=["app.retry_demo"],
        strategies={"app.retry_demo": {"strategy": "append_only"}},
    )
    retry_bundle = build_approved_execution_bundle(approval_path=retry_approval)
    retry_state_path = tmp_path / "retry_state.json"
    with pytest.raises(psycopg2.Error):
        execute(cfg=cfg, bundle=retry_bundle, state_path=str(retry_state_path))

    copy_step_id = next(
        step["id"] for step in retry_bundle.filtered_plan["steps"] if step["op"] == "copy_table"
    )
    failed_state = json.loads(retry_state_path.read_text(encoding="utf-8"))
    assert failed_state["completed"][copy_step_id]["status"] == "failed"
    _execute_sql(cfg["target"], "DELETE FROM app.retry_demo;")
    execute(cfg=cfg, bundle=retry_bundle, state_path=str(retry_state_path))

    succeeded_state = json.loads(retry_state_path.read_text(encoding="utf-8"))
    attempts = succeeded_state["completed"][copy_step_id]["attempts"]
    assert [attempt["status"] for attempt in attempts] == ["failed", "succeeded"]
    assert _fetch_all(cfg["target"], "SELECT id, value FROM app.retry_demo ORDER BY id") == [
        (1, "source-retry")
    ]
