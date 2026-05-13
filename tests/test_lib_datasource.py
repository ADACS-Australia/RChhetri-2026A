import time
from pathlib import Path
import pytest
from needle.lib.datasource import LocalDataSource, DirectorySnapshot, DataSource

def test_directory_snapshot(tmp_path):
    """Test DirectorySnapshot calculation in LocalDataSource."""
    obs_dir = tmp_path / "obs1"
    obs_dir.mkdir()
    (obs_dir / "file1.txt").write_text("hello")
    (obs_dir / "file2.txt").write_text("world")
    
    source = LocalDataSource(watch_dir=tmp_path)
    snapshot = source.snapshot("obs1")
    
    assert snapshot.file_count == 2
    assert snapshot.total_size == 10

def test_local_data_source_list_entries(tmp_path):
    """Test listing entries in LocalDataSource."""
    (tmp_path / "obs1").mkdir()
    (tmp_path / "obs2").mkdir()
    (tmp_path / "not_a_dir").touch()
    
    source = LocalDataSource(watch_dir=tmp_path)
    entries = source.list_entries()
    
    assert set(entries) == {"obs1", "obs2"}

def test_get_ready_entries_stability(tmp_path):
    """Test that entries only become ready after stability period."""
    obs_dir = tmp_path / "obs1"
    obs_dir.mkdir()
    (obs_dir / "data.ms").mkdir()
    
    source = LocalDataSource(watch_dir=tmp_path)
    
    # First check: should not be ready, but snapshot should be taken
    ready = source.get_ready_entries(stability_check=1)
    assert len(ready) == 0
    assert "obs1" in source.snapshots
    
    # Wait for stability check
    time.sleep(1.1)
    
    # Second check: should be ready
    ready = source.get_ready_entries(stability_check=1)
    assert ready == ["obs1"]
    # Once ready, snapshot is removed
    assert "obs1" not in source.snapshots

def test_get_ready_entries_resets_on_change(tmp_path):
    """Test that stability timer resets if entry changes."""
    obs_dir = tmp_path / "obs1"
    obs_dir.mkdir()
    (obs_dir / "file1").touch()
    
    source = LocalDataSource(watch_dir=tmp_path)
    
    source.get_ready_entries(stability_check=10)
    last_stable_since = source.snapshots["obs1"][1]
    
    time.sleep(0.1)
    
    # Change the directory
    (obs_dir / "file2").touch()
    
    source.get_ready_entries(stability_check=10)
    new_stable_since = source.snapshots["obs1"][1]
    
    assert new_stable_since > last_stable_since

def test_mark_received(tmp_path):
    """Test that marked entries are not returned as ready."""
    obs_dir = tmp_path / "obs1"
    obs_dir.mkdir()
    (obs_dir / "file1").touch()
    
    source = LocalDataSource(watch_dir=tmp_path)
    
    # Make it ready
    source.get_ready_entries(stability_check=0)
    ready = source.get_ready_entries(stability_check=0)
    assert ready == ["obs1"]
    
    # Mark received
    source.mark_received("obs1")
    assert "obs1" in source.received
    
    # Should no longer be ready
    ready = source.get_ready_entries(stability_check=0)
    assert len(ready) == 0
    
    # If it disappears and reappears, it should be ready again
    shutil_rmtree = __import__("shutil").rmtree
    shutil_rmtree(obs_dir)
    source.get_ready_entries(stability_check=0)
    assert "obs1" not in source.received
    
    obs_dir.mkdir()
    (obs_dir / "file1").touch()
    source.get_ready_entries(stability_check=0) # Snapshot it
    ready = source.get_ready_entries(stability_check=0)
    assert ready == ["obs1"]

def test_data_source_from_str(tmp_path):
    """Test factory method for DataSource."""
    s3_source = DataSource.from_str("s3://my-bucket/prefix")
    from needle.lib.datasource import S3DataSource
    assert isinstance(s3_source, S3DataSource)
    assert s3_source.bucket == "my-bucket"
    assert s3_source.prefix == "prefix/"
    
    local_source = DataSource.from_str(f"local://{tmp_path}")
    assert isinstance(local_source, LocalDataSource)
    assert local_source.watch_dir == tmp_path

    local_source2 = DataSource.from_str(str(tmp_path))
    assert isinstance(local_source2, LocalDataSource)
    assert local_source2.watch_dir == tmp_path
