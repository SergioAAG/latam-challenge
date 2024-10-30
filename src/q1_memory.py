import duckdb
import logging
from typing import List, Tuple, Optional
from datetime import date

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_top_user_for_date(
   con: duckdb.DuckDBPyConnection, 
   file_path: str, 
   tweet_date: date
) -> Tuple[Optional[str], int]:
   """
   Processes a specific date to identify the user with the highest number of tweets.

   Parameters
   ----------
   con : duckdb.DuckDBPyConnection
       Active connection to DuckDB.
   file_path : str
       Path to the Parquet file containing tweet data.
   tweet_date : date
       The date for which to process tweets.

   Returns
   -------
   Tuple[Optional[str], int]
       A tuple containing:
           - Username with the highest tweet count (None if no tweets found)
           - The count of tweets made by the user (0 if no tweets found)

   Raises
   ------
   duckdb.Error
       If there is an error in the DuckDB operations.
   Exception
       If an unexpected error occurs during processing.
   """
   try:
       query = f"""
       WITH RankedUsers AS (
           SELECT
               username,
               COUNT(*) AS tweet_count,
               ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC, username ASC) as rn
           FROM read_parquet('{file_path}')
           WHERE date_trunc('day', date) = '{tweet_date}'
           GROUP BY username
           ORDER BY tweet_count DESC, username ASC
       )
       SELECT username, tweet_count
       FROM RankedUsers
       WHERE rn = 1
       """
       result = con.execute(query).fetchone()

       if result:
           logger.debug(f"Top user for {tweet_date}: {result[0]} with {result[1]} tweets.")
           return result[0], result[1]
       logger.debug(f"No tweets found for date {tweet_date}.")
       return None, 0

   except Exception as e:
       logger.error(f"Error processing date {tweet_date}: {e}")
       raise

def q1_memory(file_path: str, num_threads: int = 1) -> List[Tuple[date, str]]:
   """
   Processes a Parquet file of tweets to identify the top 10 dates with the most tweets
   and, for each date, the user with the highest number of tweets.

   This function processes with temporary views instead of loading the entire file into memory.

   Parameters
   ----------
   file_path : str
       Path to the Parquet file.
   num_threads : int, optional
       Number of threads to use for parallel processing (default is 1).

   Returns
   -------
   List[Tuple[date, str]]
       A list of tuples containing:
           - Date (date): The date of the tweets
           - Username (str): The user with the most tweets on that date
       Returns an empty list if no data is found or in case of error.

   Raises
   ------
   FileNotFoundError
       If the specified file does not exist.
   duckdb.Error
       If there is an error in the DuckDB operations.
   Exception
       If an unexpected error occurs during processing.
   """
   con = None
   try:
       logger.info(f"Starting processing for file: {file_path} with {num_threads} threads")
       con = duckdb.connect(database=':memory:')
       con.execute(f"PRAGMA threads={num_threads};")

       con.execute(f"""
           CREATE TEMP VIEW top_dates AS
           SELECT 
               date_trunc('day', date) AS tweet_date,
               COUNT(*) AS tweet_count
           FROM read_parquet('{file_path}')
           GROUP BY tweet_date
           ORDER BY tweet_count DESC
           LIMIT 10;
       """)

       top_dates = con.execute("SELECT * FROM top_dates;").fetchall()

       if not top_dates:
           logger.warning(f"No tweet dates found in the file: {file_path}")
           return []

       results = []
       for tweet_date, _ in top_dates:
           try:
               top_user, _ = get_top_user_for_date(con, file_path, tweet_date)
               if top_user:
                   results.append((tweet_date, top_user))
           except Exception as e:
               logger.error(f"Failed to retrieve top user for date {tweet_date}: {e}")

       return results

   except Exception as e:
       logger.error(f"Error during processing: {e}")
       raise
   finally:
       if con:
           con.close()
