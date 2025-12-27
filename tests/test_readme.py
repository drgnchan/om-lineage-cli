from pathlib import Path


def test_readme_mentions_usage():
    text = Path("README.md").read_text(encoding="utf-8")
    assert "om-lineage" in text
    assert "--sql-file" in text


def test_readme_mentions_offline_dry_run():
    text = Path("README.md").read_text(encoding="utf-8")
    assert "dry-run" in text
    assert "openmetadata-url" in text
