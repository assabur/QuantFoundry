from pathlib import Path
import pytest
from src.utils.config_handler import ConfigHandler



def test_list_of_files_yaml_yml(tmp_path: Path):
    d = tmp_path / "cfg"
    d.mkdir()
    (d / "a.yaml").write_text("x: 1", encoding="utf-8")
    (d / "b.yml").write_text("y: 2", encoding="utf-8")
    (d / "c.txt").write_text("nope", encoding="utf-8")
    files = ConfigHandler.list_of_files(str(d))
    assert set(files) == {"a.yaml", "b.yml"}
