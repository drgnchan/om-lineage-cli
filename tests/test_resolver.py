import pytest
from om_lineage_cli.resolver import resolve_lineage, _split_table
from om_lineage_cli.sql_parser import parse_sql


class FakeMeta:
    def __init__(self):
        self.tables = {
            "svc.db.db.users": ["id", "name"],
            "svc.db.db.users_archive": ["id", "name"],
            "svc.db.db.user_out": ["id", "name"],
        }

    def get_table_columns(self, table_fqn: str) -> list[str]:
        return self.tables[table_fqn]

    def table_exists(self, table_fqn: str) -> bool:
        return table_fqn in self.tables


def test_select_star_aligns_columns():
    parsed = parse_sql("INSERT INTO db.user_out SELECT * FROM db.users")
    graph = resolve_lineage(
        parsed=parsed,
        service="svc",
        default_database="db",
        default_schema="db",
        metadata=FakeMeta(),
    )
    assert [c.target.name for c in graph.column_lineage] == ["id", "name"]
    assert str(graph.column_lineage[0].sources[0]) == "db.db.users.id"


def test_select_columns_with_alias():
    parsed = parse_sql("INSERT INTO db.user_out (id, name) SELECT u.id, u.name FROM db.users u")
    graph = resolve_lineage(
        parsed=parsed,
        service="svc",
        default_database="db",
        default_schema="db",
        metadata=FakeMeta(),
    )
    assert str(graph.column_lineage[1].sources[0]) == "db.db.users.name"


def test_ambiguous_unqualified_column_errors():
    parsed = parse_sql(
        "INSERT INTO db.user_out SELECT id FROM db.users u JOIN db.users_archive a ON u.id = a.id"
    )
    with pytest.raises(ValueError):
        resolve_lineage(
            parsed=parsed,
            service="svc",
            default_database="db",
            default_schema="db",
            metadata=FakeMeta(),
        )


def test_split_table_two_part_uses_default_db_for_database():
    t = _split_table("analytics_flink.user_info", default_database="internal", default_schema="internal")
    assert t.database == "internal"
    assert t.schema == "analytics_flink"
    assert t.name == "user_info"
