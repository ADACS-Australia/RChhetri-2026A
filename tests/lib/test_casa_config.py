import importlib.util
import os
import sys
import time
from pathlib import Path

import pytest
import yaml


@pytest.fixture
def load_casa_config(request):
    """Returns a function that (re)loads needle/lib/casa_config.py fresh. Restores the process umask afterwards."""
    from importlib.resources import files

    module_path = files("needle") / "lib" / "casa_config.py"
    loaded_names = []

    # os.umask can only be read by setting it, so set and immediately restore
    original_umask = os.umask(0)
    os.umask(original_umask)

    def _load():
        name = f"needle_casa_config_{request.node.name}_{len(loaded_names)}"
        loaded_names.append(name)
        spec = importlib.util.spec_from_file_location(name, str(module_path))
        module = importlib.util.module_from_spec(spec)
        sys.modules[name] = module
        spec.loader.exec_module(module)
        return module

    yield _load

    os.umask(original_umask)
    for name in loaded_names:
        sys.modules.pop(name, None)


def _write_needle_config(path, data_dir, staging_dir):
    path.write_text(yaml.dump({"data": {"staging_dir": str(staging_dir), "casa_dir": str(data_dir)}}))


def test_casa_config_success_sets_attrs_and_creates_dirs(tmp_path, monkeypatch, load_casa_config):
    data_dir = tmp_path / "casa_data"
    staging_dir = tmp_path / "staging"
    needle_cfg_path = tmp_path / ".needle.yaml"
    _write_needle_config(needle_cfg_path, data_dir, staging_dir)

    monkeypatch.setenv("NEEDLE_CONFIG", str(needle_cfg_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USER", "tester")

    module = load_casa_config()

    assert module.data_dir == str(data_dir)
    assert module.rundata == f"{data_dir}/.casa"
    assert module.measurespath == f"{data_dir}/casadata"
    assert module.logs_dir == f"{staging_dir}/casalogs"
    assert module.nologfile is False
    assert module.log2term is True
    assert module.nologger is True
    assert module.nogui is True
    assert module.pipeline is True

    # directories should actually have been created on disk
    assert Path(module.rundata).is_dir()
    assert Path(module.measurespath).is_dir()
    assert Path(module.logs_dir).is_dir()

    expected_ts = time.strftime("%Y%m%d-%H", time.localtime())
    assert module.logfile == f"{staging_dir}/casalogs/casalog-{expected_ts}.log"


def test_casa_config_missing_needle_config_file_raises(tmp_path, monkeypatch, capsys, load_casa_config):
    missing_path = tmp_path / "does_not_exist.yaml"
    monkeypatch.setenv("NEEDLE_CONFIG", str(missing_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USER", "tester")

    with pytest.raises(FileNotFoundError, match="Could not find NEEDLE_CONFIG file"):
        load_casa_config()

    captured = capsys.readouterr()
    assert "ERROR loading casa config file" in captured.out


def test_casa_config_missing_casa_dir_key_raises(tmp_path, monkeypatch, capsys, load_casa_config):
    needle_cfg_path = tmp_path / ".needle.yaml"
    needle_cfg_path.write_text(yaml.dump({"data": {}}))  # no casa_dir key

    monkeypatch.setenv("NEEDLE_CONFIG", str(needle_cfg_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USER", "tester")

    with pytest.raises(KeyError, match="data.casa_dir"):
        load_casa_config()

    captured = capsys.readouterr()
    assert "ERROR loading casa config file" in captured.out


def test_casa_config_created_dirs_have_correct_permissions(tmp_path, monkeypatch, load_casa_config):
    data_dir = tmp_path / "casa_data"
    staging_dir = tmp_path / "staging"
    needle_cfg_path = tmp_path / ".needle.yaml"
    _write_needle_config(needle_cfg_path, data_dir, staging_dir)

    monkeypatch.setenv("NEEDLE_CONFIG", str(needle_cfg_path))
    monkeypatch.setenv("HOME", str(tmp_path))
    monkeypatch.setenv("USER", "tester")

    module = load_casa_config()

    current_umask = os.umask(0)
    os.umask(current_umask)
    assert current_umask == 0o002, f"Expected umask 0o002, got {oct(current_umask)}"

    for created_dir in (module.rundata, module.measurespath, module.logs_dir):
        actual_mode = oct(Path(created_dir).stat().st_mode)[-3:]
        assert actual_mode == "770", f"{created_dir} has mode {actual_mode}, expected 770"
