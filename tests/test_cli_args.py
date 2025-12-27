from om_lineage_cli.cli import parse_args


def test_defaults_schema_equals_database():
    args = parse_args([
        "--sql-file", "query.sql",
        "--service", "svc",
        "--default-database", "db",
        "--openmetadata-url", "http://om",
        "--token", "t"
    ])
    assert args.default_schema == "db"
