import pytest
from pathlib import Path
from pydantic import BaseModel
from needle.lib.flow import _pydantic_cache_key

class MockModel(BaseModel):
    name: str
    value: int

def test_pydantic_cache_key_stable():
    args1 = {"a": 1, "b": "test", "c": Path("/tmp/test")}
    args2 = {"b": "test", "a": 1, "c": Path("/tmp/test")}
    
    key1 = _pydantic_cache_key(None, args1)
    key2 = _pydantic_cache_key(None, args2)
    
    assert key1 == key2
    assert isinstance(key1, str)
    assert len(key1) == 32 # MD5 hash length

def test_pydantic_cache_key_with_model():
    model = MockModel(name="test", value=10)
    args = {"model": model}
    
    key1 = _pydantic_cache_key(None, args)
    
    # Changing model should change key
    model2 = MockModel(name="test", value=11)
    key2 = _pydantic_cache_key(None, {"model": model2})
    
    assert key1 != key2

def test_pydantic_cache_key_different_types():
    key1 = _pydantic_cache_key(None, {"a": 1})
    key2 = _pydantic_cache_key(None, {"a": "1"})
    
    assert key1 != key2
