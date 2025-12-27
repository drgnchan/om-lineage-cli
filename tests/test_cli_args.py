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


def test_dry_run_allows_missing_openmetadata_config():
    args = parse_args([
        "--sql-file", "query.sql",
        "--service", "svc",
        "--default-database", "db",
        "--dry-run",
    ])
    assert args.openmetadata_url is None
    assert args.token is None
