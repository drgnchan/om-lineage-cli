from dataclasses import dataclass
from typing import Tuple


@dataclass(frozen=True)
class TableName:
    database: str
    schema: str
    name: str

    def qualified_name(self) -> str:
        return f"{self.database}.{self.schema}.{self.name}"


@dataclass(frozen=True)
class ColumnRef:
    table: TableName
    name: str

    def __str__(self) -> str:
        return f"{self.table.qualified_name()}.{self.name}"


@dataclass(frozen=True)
class ColumnLineage:
    target: ColumnRef
    sources: Tuple[ColumnRef, ...]


@dataclass(frozen=True)
class LineageGraph:
    target_table: TableName
    source_tables: Tuple[TableName, ...]
    column_lineage: Tuple[ColumnLineage, ...]


@dataclass(frozen=True)
class TableEntity:
    id: str
    fqn: str
    columns: dict[str, str]  # column name -> column FQN
