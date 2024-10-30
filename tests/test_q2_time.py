import pytest
from src.q2_time import q2_time

empty_parquet_file_path = "tests/resources/empty_tweets.parquet"
test_parquet_file_path = 'tests/resources/small_tweets.parquet'

def test_q2_time_basic_functionality():
    """
    Tests the basic functionality of q2_time function by verifying correct data handling
    and type validation of returned results for emoji counting.

    Parameters
    ----------
    None
        Uses global test_parquet_file_path for testing.

    Returns
    -------
    None
        Test passes if all assertions are successful.

    Raises
    ------
    AssertionError
        If any of the following conditions fail:
        - Result is not a list
        - Results items are not tuples
        - Tuples don't contain exactly 2 elements
        - First tuple element is not a string (emoji)
        - Second tuple element is not an integer (count)
        - Results don't match expected emoji counts output
    """    
    results = q2_time(test_parquet_file_path)

    assert isinstance(results, list), "Result must be a list"
    assert all(isinstance(item, tuple) for item in results), "Each result must be a tuple"
    assert all(len(item) == 2 for item in results), "Each tuple must have two elements"
    
    assert all(isinstance(item[0], str) for item in results), "First element must be a string"
    assert all(isinstance(item[1], int) for item in results), "Second element must be an integer"

    expected_results = [('🤫', 2), ('🤔', 2), ('🚜', 1), ('🌾', 1), ('💪', 1)]
    assert results == expected_results, "Results do not match expected output"

def test_q2_time_empty_file():
    """
    Tests q2_time function behavior when processing an empty Parquet file.

    Parameters
    ----------
    None
        Uses global empty_parquet_file_path for testing.

    Returns
    -------
    None
        Test passes if assertion is successful.

    Raises
    ------
    AssertionError
        If the function doesn't return an empty list when processing an empty file.
    """
    results = q2_time(empty_parquet_file_path)
    assert len(results) == 0, "Empty file should return an empty list"