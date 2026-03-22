from __future__ import annotations

import json

from amo.core import verifier


class DummyCursor:
    def __init__(self, connection):
        self.connection = connection
        self.fetchone_result = None
        self.fetchall_result = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def execute(self, query, params=None):
        text = str(query)
        self.connection.executed.append((text, params))
        if "COUNT(*)" in text:
            self.fetchone_result = (5,)
            self.fetchall_result = []
        elif "SELECT kcu.column_name" in text:
            self.fetchone_result = None
            self.fetchall_result = []
        elif "SELECT * FROM" in text:
            self.fetchone_result = None
            self.fetchall_result = [(1,), (2,)]
        else:
            self.fetchone_result = None
            self.fetchall_result = []

    def fetchone(self):
        return self.fetchone_result

    def fetchall(self):
        return self.fetchall_result


class DummyConnection:
    def __init__(self):
        self.autocommit = False
        self.executed = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def cursor(self):
        return DummyCursor(self)


def test_verify_plan_prefers_verify_steps_over_copy_steps(tmp_path, monkeypatch):
    plan_path = tmp_path / "plan.json"
    plan_path.write_text(
        json.dumps(
            {
                "steps": [
                    {"id": "1", "op": "copy_table", "schema": "public", "table": "users"},
                    {"id": "2", "op": "verify_table", "schema": "public", "table": "users", "validate": {}},
                ]
            }
        )
    )

    def fake_connect(*args, **kwargs):
        return DummyConnection()

    monkeypatch.setattr(verifier.psycopg2, "connect", fake_connect)
    report = verifier.verify_plan(
        cfg={
            "source": {"host": "src", "database": "demo", "user": "u", "password": "p"},
            "target": {"host": "dst", "database": "demo", "user": "u", "password": "p"},
        },
        plan_path=str(plan_path),
    )

    assert report["tables_checked"] == 1
    assert len(report["results"]) == 1
