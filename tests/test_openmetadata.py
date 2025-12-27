import responses
from om_lineage_cli.openmetadata import OpenMetadataClient


@responses.activate
def test_get_table_entity():
    client = OpenMetadataClient(base_url="http://om", token="t")
    fqn = "svc.db.db.users"

    responses.add(
        responses.GET,
        "http://om/api/v1/tables/name/svc.db.db.users",
        json={
            "id": "123",
            "fullyQualifiedName": fqn,
            "columns": [{"name": "id"}, {"name": "name"}],
        },
        status=200,
    )

    table = client.get_table(fqn)
    assert table.id == "123"
    assert table.columns["id"].endswith(".id")
