from om_lineage_cli.models import LineageGraph


def format_dry_run(graph: LineageGraph) -> str:
    lines: list[str] = []
    lines.append(f"Target: {graph.target_table.qualified_name()}")
    for src in graph.source_tables:
        lines.append(f"Source: {src.qualified_name()}")
    if graph.column_lineage:
        lines.append("Columns:")
        for cl in graph.column_lineage:
            srcs = ", ".join(str(s) for s in cl.sources)
            lines.append(f"  {cl.target.name} <- {srcs}")
    return "\n".join(lines)
