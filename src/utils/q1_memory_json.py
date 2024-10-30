import duckdb
import datetime
from typing import List, Tuple

def q1_memory_json(file_path: str) -> List[Tuple[datetime.date, str]]:
   """
   Processes a JSONL file containing tweet data to find the users with the most tweets
   for each of the top 10 days by tweet volume. Uses memory optimization through chunked processing.

   Parameters
   ----------
   file_path : str
       Path to the JSONL file.

   Returns
   -------
   List[Tuple[datetime.date, str]]
       A list of tuples containing:
       - tweet_date (datetime.date): The date of the tweets
       - username (str): The username of the most active user on that date
       The list is sorted by total tweet count per date in descending order,
       limited to the top 10 dates.

   Raises
   ------
   FileNotFoundError
       If the specified file does not exist.
   duckdb.BinderException
       If there is an error in the file structure or SQL queries.
   duckdb.Error
       If there are issues with database operations.
   ValueError
       If the date format in the JSON file is invalid.
   MemoryError
       If there is insufficient memory to process the data chunks.
   Exception
       If an unexpected error occurs during processing.
   """
   con = duckdb.connect(database=':memory:')

   con.execute("""
   CREATE TABLE IF NOT EXISTS tweets (
       tweet_date DATE,
       username VARCHAR
   );
   """)

   try:
       offset = 0
       chunk_size = 200000 
       while True:
           query = f"""
           INSERT INTO tweets
           SELECT
               date_trunc('day', CAST(date AS TIMESTAMP))::DATE AS tweet_date,
               user->>'username' AS username
           FROM read_json_auto('{file_path}', maximum_object_size=1000000)
           LIMIT {chunk_size} OFFSET {offset};
           """
           con.execute(query)

           if con.execute("SELECT COUNT(*) FROM tweets;").fetchone()[0] < offset + chunk_size:
               break
           offset += chunk_size

       top_dates = con.execute("""
       SELECT
           tweet_date,
           COUNT(*) AS tweet_count
       FROM tweets
       GROUP BY tweet_date
       ORDER BY tweet_count DESC
       LIMIT 10;
       """).fetchall()

       results = []
       for row in top_dates:
           tweet_date = row[0]
           top_user = con.execute(f"""
           SELECT
               username,
               COUNT(*) AS user_tweet_count
           FROM tweets
           WHERE tweet_date = '{tweet_date}'
           GROUP BY username
           ORDER BY user_tweet_count DESC
           LIMIT 1;
           """).fetchone()
           results.append((tweet_date, top_user[0]))

   except duckdb.BinderException:
       con.close()
       return []
   except Exception as e:
       con.close()
       raise e

   con.close()
   return results
