from om_lineage_cli.cli import build_lineage_payload
from om_lineage_cli.models import TableEntity, TableName, ColumnRef, ColumnLineage, LineageGraph


def test_build_lineage_payload():
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
    table_map = {
        "svc.db.db.out": TableEntity(id="1", fqn="svc.db.db.out", columns={"id": "svc.db.db.out.id"}),
        "svc.db.db.src": TableEntity(id="2", fqn="svc.db.db.src", columns={"id": "svc.db.db.src.id"}),
    }

    payload = build_lineage_payload(graph, service="svc", table_map=table_map)
    assert payload["edge"]["toEntity"]["fullyQualifiedName"] == "svc.db.db.out"
    assert payload["edge"]["lineageDetails"]["columnsLineage"][0]["toColumn"]["fullyQualifiedName"].endswith(".id")
