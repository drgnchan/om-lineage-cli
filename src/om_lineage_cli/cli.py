import argparse
from pathlib import Path
from om_lineage_cli.config import Config
from om_lineage_cli.formatter import format_dry_run
from om_lineage_cli.models import LineageGraph, TableEntity, TableName
from om_lineage_cli.openmetadata import OpenMetadataClient
from om_lineage_cli.resolver import resolve_lineage
from om_lineage_cli.sql_parser import parse_sql


def parse_args(argv: list[str]) -> Config:
    parser = argparse.ArgumentParser(prog="om-lineage")
    parser.add_argument("--sql-file", required=True)
    parser.add_argument("--service", required=True)
    parser.add_argument("--default-database", required=True)
    parser.add_argument("--default-schema")
    parser.add_argument("--openmetadata-url")
    parser.add_argument("--token")
    parser.add_argument("--target")
    parser.add_argument("--dry-run", action="store_true")

    ns = parser.parse_args(argv)
    default_schema = ns.default_schema or ns.default_database

    if not ns.dry_run and (not ns.openmetadata_url or not ns.token):
        parser.error("--openmetadata-url and --token are required unless --dry-run is set")

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


def _split_table(name: str, default_database: str, default_schema: str):
    parts = name.split(".")
    if len(parts) == 1:
        return TableName(database=default_database, schema=default_schema, name=parts[0])
    if len(parts) == 2:
        return TableName(database=default_database, schema=parts[0], name=parts[1])
    if len(parts) == 3:
        return TableName(database=parts[0], schema=parts[1], name=parts[2])
    raise ValueError(f"Unsupported table name: {name}")


def run_once(
    *,
    sql_file: str,
    service: str,
    default_database: str,
    default_schema: str,
    target: str | None,
    dry_run: bool,
    om_client: OpenMetadataClient | None,
) -> str | None:
    sql = Path(sql_file).read_text(encoding="utf-8")
    parsed = parse_sql(sql)

    if parsed.is_select_only and not target:
        raise SystemExit("--target is required for SELECT-only SQL")

    if target:
        parsed = parsed.__class__(
            target=target,
            sources=parsed.sources,
            select_expressions=parsed.select_expressions,
            insert_columns=parsed.insert_columns,
            is_select_only=parsed.is_select_only,
            cte_names=parsed.cte_names,
            table_aliases=parsed.table_aliases,
        )

    
    if dry_run and om_client is None:
        target_name = parsed.target or target
        if target_name is None:
            raise SystemExit("--target is required for SELECT-only SQL")
        target_table = _split_table(target_name, default_database, default_schema)
        source_tables = tuple(_split_table(s, default_database, default_schema) for s in parsed.sources)
        graph = LineageGraph(
            target_table=target_table,
            source_tables=source_tables,
            column_lineage=tuple(),
        )
        return format_dry_run(graph)

    graph = resolve_lineage(
        parsed=parsed,
        service=service,
        default_database=default_database,
        default_schema=default_schema,
        metadata=om_client,
    )

    table_map: dict[str, TableEntity] = {}
    target_fqn = f"{service}.{graph.target_table.qualified_name()}"
    table_map[target_fqn] = om_client.get_table(target_fqn)
    for src in graph.source_tables:
        src_fqn = f"{service}.{src.qualified_name()}"
        table_map[src_fqn] = om_client.get_table(src_fqn)

    if dry_run:
        return format_dry_run(graph)

    payload = build_lineage_payload(graph, service=service, table_map=table_map)
    om_client.post_lineage(payload)
    return None


def main() -> None:
    config = parse_args(None)
    client = None
    if not config.dry_run:
        client = OpenMetadataClient(config.openmetadata_url, config.token)
    output = run_once(
        sql_file=config.sql_file,
        service=config.service,
        default_database=config.default_database,
        default_schema=config.default_schema,
        target=config.target,
        dry_run=config.dry_run,
        om_client=client,
    )
    if output:
        print(output)
