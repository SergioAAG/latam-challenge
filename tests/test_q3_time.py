import pytest
from src.q3_time import q3_time

empty_parquet_file_path = "tests/resources/empty_tweets.parquet"
test_parquet_file_path = 'tests/resources/small_tweets.parquet'

def test_q3_time_basic_functionality():
   """
   Tests the basic functionality of q3_time function by verifying correct data handling
   and type validation of returned results for user mention counting.

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
       - First tuple element is not a string (username)
       - Second tuple element is not an integer (mention count)
       - Results don't match expected user mentions output
   """    
   results = q3_time(test_parquet_file_path)

   assert isinstance(results, list), "Result must be a list"
   assert all(isinstance(item, tuple) for item in results), "Each result must be a tuple"
   assert all(len(item) == 2 for item in results), "Each tuple must have two elements"
   
   assert all(isinstance(item[0], str) for item in results), "First element must be a string"
   assert all(isinstance(item[1], int) for item in results), "Second element must be an integer"

   expected_results = [('user2', 1), ('user3', 1), ('user4', 1), ('user5', 1), ('user6', 1)]
   assert results == expected_results, "Results do not match expected output"

def test_q3_time_empty_file():
   """
   Tests q3_time function behavior when processing an empty Parquet file.

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
   results = q3_time(empty_parquet_file_path)
   assert len(results) == 0, "Empty file should return an empty list"