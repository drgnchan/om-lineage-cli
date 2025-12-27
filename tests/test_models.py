from om_lineage_cli.models import TableName, ColumnRef, TableEntity


def test_table_qualified_name():
    t = TableName(database="db", schema="sch", name="tbl")
    assert t.qualified_name() == "db.sch.tbl"


def test_column_ref_str():
    t = TableName(database="db", schema="sch", name="tbl")
    c = ColumnRef(table=t, name="col")
    assert str(c) == "db.sch.tbl.col"


def test_table_entity_fqn():
    t = TableEntity(id="1", fqn="svc.db.sch.tbl", columns={"id": "svc.db.sch.tbl.id"})
    assert t.columns["id"].endswith(".id")
