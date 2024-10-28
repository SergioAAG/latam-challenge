import pytest
import datetime
from src.q1_time import q1_time

empty_parquet_file_path = "tests/resources/empty_tweets.parquet"
test_parquet_file_path = 'tests/resources/small_tweets.parquet'

def test_q1_time_basic_functionality():
    """Test q1_time to verify that it handles valid data and returns correct types."""    
    results = q1_time(test_parquet_file_path)

    assert isinstance(results, list), "Result must be a list"
    assert all(isinstance(item, tuple) for item in results), "Each result must be a tuple"
    assert all(len(item) == 2 for item in results), "Each tuple must have two elements"
    
    assert all(isinstance(item[0], datetime.date) for item in results), "First element must be a date"
    assert all(isinstance(item[1], str) for item in results), "Second element must be a string"

    expected_results = [(datetime.date(2021, 2, 24), 'user1'), (datetime.date(2021, 2, 25), 'user5')]
    assert results == expected_results, "Results do not match expected output"

def test_q1_time_empty_file():
    """Test q1_time with an empty Parquet file."""
    results = q1_time(empty_parquet_file_path)
    assert len(results) == 0, "Empty file should return an empty list"