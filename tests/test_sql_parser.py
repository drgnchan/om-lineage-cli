import pytest
from om_lineage_cli.sql_parser import parse_sql


def test_reject_multiple_statements():
    with pytest.raises(ValueError):
        parse_sql("SELECT 1; SELECT 2;")


def test_parse_insert_with_cte_and_union():
    sql = """
    WITH cte AS (
      SELECT id, name FROM src.users
    )
    INSERT INTO analytics.user_out
    SELECT id, name FROM cte
    UNION ALL
    SELECT id, name FROM src.users_archive
    """.strip()

    parsed = parse_sql(sql)
    assert parsed.target == "analytics.user_out"
    assert sorted(parsed.sources) == ["src.users", "src.users_archive"]
    assert "cte" in parsed.cte_names
