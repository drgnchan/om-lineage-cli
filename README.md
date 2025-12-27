# OpenMetadata Lineage CLI

Parse a single MySQL SQL statement from a file and write table/column lineage to OpenMetadata 1.11.4.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e .[dev]
```

## Usage

```bash
om-lineage \
  --sql-file ./query.sql \
  --service my_service \
  --default-database analytics \
  --openmetadata-url http://localhost:8585 \
  --token "$OPENMETADATA_TOKEN"
```

If SQL is SELECT-only, provide a target:

```bash
om-lineage \
  --sql-file ./query.sql \
  --service my_service \
  --default-database analytics \
  --target analytics.public.daily_kpi \
  --openmetadata-url http://localhost:8585 \
  --token "$OPENMETADATA_TOKEN"
```

If you only want a table-level preview without OpenMetadata credentials:

```bash
om-lineage \
  --sql-file ./query.sql \
  --service my_service \
  --default-database analytics \
  --dry-run
```

When not using `--dry-run`, `--openmetadata-url` and `--token` are required.
