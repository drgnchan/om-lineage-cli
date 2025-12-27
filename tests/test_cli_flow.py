import tempfile
from pathlib import Path
from om_lineage_cli.cli import run_once


class FakeOM:
    def __init__(self):
        self.tables = {
            "svc.db.db.users": {"id": "1", "fullyQualifiedName": "svc.db.db.users", "columns": [{"name": "id"}]},
            "svc.db.db.user_out": {"id": "2", "fullyQualifiedName": "svc.db.db.user_out", "columns": [{"name": "id"}]},
        }
        self.posted = None

    def table_exists(self, fqn: str) -> bool:
        return fqn in self.tables

    def get_table_columns(self, fqn: str) -> list[str]:
        return [c["name"] for c in self.tables[fqn]["columns"]]

    def get_table(self, fqn: str):
        data = self.tables[fqn]
        columns = {c["name"]: f"{data['fullyQualifiedName']}.{c['name']}" for c in data["columns"]}
        from om_lineage_cli.models import TableEntity
        return TableEntity(id=data["id"], fqn=data["fullyQualifiedName"], columns=columns)

    def post_lineage(self, payload: dict) -> None:
        self.posted = payload


def test_run_once_dry_run():
    sql = "INSERT INTO db.user_out SELECT * FROM db.users"
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "q.sql"
        path.write_text(sql, encoding="utf-8")
        out = run_once(
            sql_file=str(path),
            service="svc",
            default_database="db",
            default_schema="db",
            target=None,
            dry_run=True,
            om_client=FakeOM(),
        )
        assert "Target" in out


def test_run_once_offline_dry_run_skips_column_lineage():
    sql = "INSERT INTO db.user_out SELECT * FROM db.users"
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "q.sql"
        path.write_text(sql, encoding="utf-8")
        out = run_once(
            sql_file=str(path),
            service="svc",
            default_database="db",
            default_schema="db",
            target=None,
            dry_run=True,
            om_client=None,
        )
        assert "Target: db.db.user_out" in out
        assert "Source: db.db.users" in out
        assert "Columns:" not in out
