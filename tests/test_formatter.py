from om_lineage_cli.models import TableName, ColumnRef, ColumnLineage, LineageGraph
from om_lineage_cli.formatter import format_dry_run


def test_format_dry_run():
    target = TableName(database="db", schema="db", name="out")
    source = TableName(database="db", schema="db", name="src")
    graph = LineageGraph(
        target_table=target,
        source_tables=(source,),
        column_lineage=(
            ColumnLineage(
                target=ColumnRef(table=target, name="id"),
                sources=(ColumnRef(table=source, name="id"),),
            ),
        ),
    )
    text = format_dry_run(graph)
    assert "Target: db.db.out" in text
    assert "Source: db.db.src" in text
    assert "id <- db.db.src.id" in text
