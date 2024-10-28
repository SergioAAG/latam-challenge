import pytest
from src.q2_memory import q2_memory

empty_parquet_file_path = "tests/resources/empty_tweets.parquet"
test_parquet_file_path = 'tests/resources/small_tweets.parquet'

def test_q2_memory_basic_functionality():
    """Test q2_memory to verify that it handles valid data and returns correct types."""    
    results = q2_memory(test_parquet_file_path)

    assert isinstance(results, list), "Result must be a list"
    assert all(isinstance(item, tuple) for item in results), "Each result must be a tuple"
    assert all(len(item) == 2 for item in results), "Each tuple must have two elements"
    
    assert all(isinstance(item[0], str) for item in results), "First element must be a string"
    assert all(isinstance(item[1], int) for item in results), "Second element must be an integer"

    expected_results = [('🤫', 2), ('🤔', 2), ('🚜', 1), ('🌾', 1), ('💪', 1)]
    assert results == expected_results, "Results do not match expected output"

def test_q2_memory_empty_file():
    """Test q2_memory with an empty Parquet file."""
    results = q2_memory(empty_parquet_file_path)
    assert len(results) == 0, "Empty file should return an empty list"