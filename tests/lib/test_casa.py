import os
import sys
from unittest.mock import MagicMock, patch

import pytest

# Adjust this import path to match where the module actually lives
from needle.lib.casa import (
    set_casa_config,
    get_table,
    open_table,
    get_msmetadata,
    open_msmetadata,
)


def test_set_casa_config_sets_env_var(monkeypatch):
    monkeypatch.delenv("CASASITECONFIG", raising=False)

    set_casa_config()

    result = os.environ["CASASITECONFIG"]
    assert result.endswith(os.path.join("lib", "casa_config.py"))
    assert "needle" in result


def test_get_table_returns_table_instance():
    fake_table_instance = MagicMock()
    fake_casatools = MagicMock()
    fake_casatools.table.return_value = fake_table_instance

    with patch.dict(sys.modules, {"casatools": fake_casatools}):
        result = get_table()

    assert result is fake_table_instance


def test_get_table_raises_import_error_when_casatools_missing():
    # Setting a module to None in sys.modules forces `import` to raise ImportError
    with patch.dict(sys.modules, {"casatools": None}):
        with pytest.raises(ImportError, match="casatools is required to read an MS directly."):
            get_table()


def test_open_table_opens_yields_and_closes():
    fake_tb = MagicMock()

    with patch("needle.lib.casa.get_table", return_value=fake_tb):
        with open_table("some/path.ms") as tb:
            assert tb is fake_tb
            fake_tb.open.assert_called_once_with("some/path.ms")
            fake_tb.close.assert_not_called()

    fake_tb.close.assert_called_once()


def test_open_table_closes_even_on_exception():
    fake_tb = MagicMock()

    with patch("needle.lib.casa.get_table", return_value=fake_tb):
        with pytest.raises(RuntimeError):
            with open_table("some/path.ms"):
                raise RuntimeError("boom")

    fake_tb.close.assert_called_once()


def test_get_msmetadata_returns_msmetadata_instance():
    fake_md_instance = MagicMock()
    fake_casatools = MagicMock()
    fake_casatools.msmetadata.return_value = fake_md_instance

    with patch.dict(sys.modules, {"casatools": fake_casatools}):
        result = get_msmetadata()

    assert result is fake_md_instance


def test_get_msmetadata_raises_import_error_when_casatools_missing():
    with patch.dict(sys.modules, {"casatools": None}):
        with pytest.raises(ImportError, match="casatools is required to read an MS directly."):
            get_msmetadata()


def test_open_msmetadata_opens_yields_and_closes():
    fake_md = MagicMock()

    with patch("needle.lib.casa.get_msmetadata", return_value=fake_md):
        with open_msmetadata("some/path.ms") as md:
            assert md is fake_md
            fake_md.open.assert_called_once_with("some/path.ms")
            fake_md.close.assert_not_called()

    fake_md.close.assert_called_once()


def test_open_msmetadata_closes_even_on_exception():
    fake_md = MagicMock()

    with patch("needle.lib.casa.get_msmetadata", return_value=fake_md):
        with pytest.raises(RuntimeError):
            with open_msmetadata("some/path.ms"):
                raise RuntimeError("boom")

    fake_md.close.assert_called_once()
