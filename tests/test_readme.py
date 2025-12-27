from pathlib import Path


def test_readme_mentions_usage():
    text = Path("README.md").read_text(encoding="utf-8")
    assert "om-lineage" in text
    assert "--sql-file" in text
