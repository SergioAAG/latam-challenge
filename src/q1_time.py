import duckdb
import logging
from typing import List, Tuple
from datetime import date

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def q1_time(file_path: str, num_threads: int = 4) -> List[Tuple[date, str]]:
    """Gets top 10 dates with most tweets and the user with highest tweets for each date using a single query."""
    con = None
    try:
        logger.info(f"Starting processing for file: {file_path} with {num_threads} threads")
        con = duckdb.connect(database=':memory:')
        con.execute(f"PRAGMA threads={num_threads};")

        query = f"""
        WITH TopDates AS (
            SELECT
                date_trunc('day', date) AS tweet_date,
                COUNT(*) AS tweet_count
            FROM read_parquet('{file_path}')
            GROUP BY tweet_date
            ORDER BY tweet_count DESC
            LIMIT 10
        ),
        RankedUsers AS (
            SELECT
                date_trunc('day', date) AS tweet_date,
                username,
                COUNT(*) AS tweet_count,
                ROW_NUMBER() OVER (PARTITION BY date_trunc('day', date) ORDER BY COUNT(*) DESC, username ASC) AS rn
            FROM read_parquet('{file_path}')
            GROUP BY tweet_date, username
        )
        SELECT TD.tweet_date, RU.username
        FROM TopDates TD
        JOIN RankedUsers RU ON TD.tweet_date = RU.tweet_date
        WHERE RU.rn = 1
        ORDER BY TD.tweet_count DESC;
        """

        results = con.execute(query).fetchall()

        if not results:
            logger.warning(f"No results found in the file: {file_path}")
            return []

        return results

    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise
    finally:
        if con:
            con.close()
