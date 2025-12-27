from sqlglot import exp
from om_lineage_cli.models import TableName, ColumnRef, ColumnLineage, LineageGraph
from om_lineage_cli.sql_parser import ParsedSQL


class MetadataProvider:
    def table_exists(self, table_fqn: str) -> bool:  # pragma: no cover - interface
        raise NotImplementedError

    def get_table_columns(self, table_fqn: str) -> list[str]:  # pragma: no cover - interface
        raise NotImplementedError


def _split_table(name: str, default_database: str, default_schema: str) -> TableName:
    parts = name.split(".")
    if len(parts) == 1:
        return TableName(database=default_database, schema=default_schema, name=parts[0])
    if len(parts) == 2:
        return TableName(database=parts[0], schema=default_schema, name=parts[1])
    if len(parts) == 3:
        return TableName(database=parts[0], schema=parts[1], name=parts[2])
    raise ValueError(f"Unsupported table name: {name}")


def _fqn(service: str, table: TableName) -> str:
    return f"{service}.{table.database}.{table.schema}.{table.name}"


def _resolve_table_name(raw: str, table_aliases: dict[str, str]) -> str:
    return table_aliases.get(raw, raw)


def resolve_lineage(
    *,
    parsed: ParsedSQL,
    service: str,
    default_database: str,
    default_schema: str,
    metadata: MetadataProvider,
) -> LineageGraph:
    if parsed.target is None:
        raise ValueError("target table is required")

    target = _split_table(parsed.target, default_database, default_schema)
    target_fqn = _fqn(service, target)
    if not metadata.table_exists(target_fqn):
        raise ValueError(f"target table not found: {target_fqn}")

    sources = [_split_table(s, default_database, default_schema) for s in parsed.sources]
    for s in sources:
        if not metadata.table_exists(_fqn(service, s)):
            raise ValueError(f"source table not found: {_fqn(service, s)}")

    source_columns: dict[TableName, list[str]] = {}
    for s in sources:
        source_columns[s] = metadata.get_table_columns(_fqn(service, s))

    target_cols = parsed.insert_columns or metadata.get_table_columns(target_fqn)

    select_sources: list[tuple[ColumnRef, ...]] = []
    for expr in parsed.select_expressions:
        if isinstance(expr, exp.Star) or (isinstance(expr, exp.Column) and isinstance(expr.this, exp.Star)):
            if isinstance(expr, exp.Column) and expr.table:
                table_name = _resolve_table_name(expr.table, parsed.table_aliases)
                table = _split_table(table_name, default_database, default_schema)
            else:
                if len(sources) != 1:
                    raise ValueError("SELECT * with multiple sources is ambiguous")
                table = sources[0]
            for col in source_columns[table]:
                select_sources.append((ColumnRef(table=table, name=col),))
            continue

        cols = list(expr.find_all(exp.Column))
        if not cols:
            raise ValueError("expression has no source columns")

        resolved: list[ColumnRef] = []
        for col in cols:
            if col.table:
                table_name = _resolve_table_name(col.table, parsed.table_aliases)
                table = _split_table(table_name, default_database, default_schema)
                resolved.append(ColumnRef(table=table, name=col.name))
                continue

            matches = []
            for t, cols_list in source_columns.items():
                if col.name in cols_list:
                    matches.append(t)
            if len(matches) != 1:
                raise ValueError(f"ambiguous column: {col.name}")
            resolved.append(ColumnRef(table=matches[0], name=col.name))

        select_sources.append(tuple(resolved))

    if len(select_sources) != len(target_cols):
        raise ValueError("select columns and target columns length mismatch")

    column_lineage: list[ColumnLineage] = []
    for idx, tgt in enumerate(target_cols):
        column_lineage.append(
            ColumnLineage(
                target=ColumnRef(table=target, name=tgt),
                sources=select_sources[idx],
            )
        )

    return LineageGraph(
        target_table=target,
        source_tables=tuple(sources),
        column_lineage=tuple(column_lineage),
    )
