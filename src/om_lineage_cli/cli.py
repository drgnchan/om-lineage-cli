import argparse
from om_lineage_cli.config import Config


def parse_args(argv: list[str]) -> Config:
    parser = argparse.ArgumentParser(prog="om-lineage")
    parser.add_argument("--sql-file", required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--default-database", required=True)
    parser.add_argument("--default-schema")
    parser.add_argument("--openmetadata-url", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--target")
    parser.add_argument("--dry-run", action="store_true")

    ns = parser.parse_args(argv)
    default_schema = ns.default_schema or ns.default_database

    return Config(
        sql_file=ns.sql_file,
        service=ns.service,
        default_database=ns.default_database,
        default_schema=default_schema,
        openmetadata_url=ns.openmetadata_url,
        token=ns.token,
        target=ns.target,
        dry_run=ns.dry_run,
    )


def main() -> None:
    parse_args([])
