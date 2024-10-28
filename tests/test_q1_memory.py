import pytest
import datetime
from src.q1_memory import q1_memory

empty_parquet_file_path = "tests/resources/empty_tweets.parquet"
test_parquet_file_path = 'tests/resources/small_tweets.parquet'

def test_q1_memory_basic_functionality():
    """Test q1_memory to verify that it handles valid data and returns correct types."""    
    results = q1_memory(test_parquet_file_path)

    assert isinstance(results, list), "Result must be a list"
    assert all(isinstance(item, tuple) for item in results), "Each result must be a tuple"
    assert all(len(item) == 2 for item in results), "Each tuple must have two elements"
    
    assert all(isinstance(item[0], datetime.date) for item in results), "First element must be a date"
    assert all(isinstance(item[1], str) for item in results), "Second element must be a string"

    expected_results = [(datetime.date(2021, 2, 24), 'user1'), (datetime.date(2021, 2, 25), 'user5')]
    assert results == expected_results, "Results do not match expected output"

def test_q1_memory_empty_file():
    """Test q1_memorywith an empty Parquet file."""
    results = q1_memory(empty_parquet_file_path)
    assert len(results) == 0, "Empty file should return an empty list"