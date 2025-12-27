from dataclasses import dataclass
from typing import List, Dict, Set
import sqlglot
from sqlglot import exp


@dataclass(frozen=True)
class ParsedSQL:
    target: str | None
    sources: List[str]
    select_expressions: List[exp.Expression]
    insert_columns: List[str] | None
    is_select_only: bool
    cte_names: Set[str]
    table_aliases: Dict[str, str]  # alias -> table name


def _collect_cte_names(expression: exp.Expression) -> Set[str]:
    names = set()
    for cte in expression.find_all(exp.CTE):
        if cte.alias:
            names.add(cte.alias)
    return names


def _table_name(table: exp.Table) -> str:
    name = table.name
    if table.db:
        name = f"{table.db}.{name}"
    if table.catalog:
        name = f"{table.catalog}.{name}"
    return name


def _collect_sources(expression: exp.Expression, cte_names: Set[str]) -> List[str]:
    tables = set()
    for table in expression.find_all(exp.Table):
        name = _table_name(table)
        base = table.name
        if base in cte_names:
            continue
        tables.add(name)
    return sorted(tables)


def _collect_aliases(expression: exp.Expression, cte_names: Set[str]) -> Dict[str, str]:
    aliases: Dict[str, str] = {}
    for table in expression.find_all(exp.Table):
        if table.name in cte_names:
            continue
        if table.alias:
            aliases[table.alias] = _table_name(table)
    return aliases


def parse_sql(sql: str) -> ParsedSQL:
    statements = sqlglot.parse(sql, read="mysql")
    if len(statements) != 1:
        raise ValueError("SQL file must contain exactly one statement")

    parsed = statements[0]
    cte_names = _collect_cte_names(parsed)

    target = None
    insert_columns = None
    is_select_only = isinstance(parsed, exp.Select)

    if isinstance(parsed, exp.Insert):
        if isinstance(parsed.this, exp.Schema):
            target = parsed.this.this.sql(dialect="mysql")
            insert_columns = [c.name for c in parsed.this.expressions]
        else:
            target = parsed.this.sql(dialect="mysql")
        select_expr = parsed.expression
    elif isinstance(parsed, exp.Create):
        target = parsed.this.sql(dialect="mysql")
        select_expr = parsed.expression
    elif isinstance(parsed, exp.Select):
        select_expr = parsed
    else:
        select_expr = parsed

    sources = _collect_sources(parsed, cte_names)
    if target:
        sources = [s for s in sources if s != target]
    table_aliases = _collect_aliases(select_expr, cte_names)

    select_expressions = []
    if isinstance(select_expr, exp.Select):
        select_expressions = list(select_expr.expressions)
    else:
        select = select_expr.find(exp.Select)
        if select is not None:
            select_expressions = list(select.expressions)

    return ParsedSQL(
        target=target,
        sources=sources,
        select_expressions=select_expressions,
        insert_columns=insert_columns,
        is_select_only=is_select_only,
        cte_names=cte_names,
        table_aliases=table_aliases,
    )
