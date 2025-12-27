import argparse
from om_lineage_cli.config import Config
from om_lineage_cli.formatter import format_dry_run
from om_lineage_cli.models import LineageGraph, TableEntity
from om_lineage_cli.openmetadata import OpenMetadataClient
from om_lineage_cli.resolver import resolve_lineage
from om_lineage_cli.sql_parser import parse_sql


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


def build_lineage_payload(graph: LineageGraph, service: str, table_map: dict[str, TableEntity]) -> dict:
    to_fqn = f"{service}.{graph.target_table.qualified_name()}"
    to_table = table_map[to_fqn]
    columns_lineage = []

    for cl in graph.column_lineage:
        to_col_fqn = f"{to_table.fqn}.{cl.target.name}"
        from_cols = []
        for src in cl.sources:
            src_fqn = f"{service}.{src.table.qualified_name()}"
            src_table = table_map[src_fqn]
            from_cols.append({"fullyQualifiedName": f"{src_table.fqn}.{src.name}", "type": "column"})
        columns_lineage.append({
            "fromColumns": from_cols,
            "toColumn": {"fullyQualifiedName": to_col_fqn, "type": "column"},
        })

    first_source = graph.source_tables[0]
    from_fqn = f"{service}.{first_source.qualified_name()}"
    from_table = table_map[from_fqn]
    return {
        "edge": {
            "fromEntity": {"id": from_table.id, "type": "table", "fullyQualifiedName": from_table.fqn},
            "toEntity": {"id": to_table.id, "type": "table", "fullyQualifiedName": to_table.fqn},
            "lineageDetails": {"columnsLineage": columns_lineage},
        }
    }


def main() -> None:
    parse_args([])
