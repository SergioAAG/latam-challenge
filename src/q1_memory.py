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
    """Gets the user with the highest tweet count for a specific date."""
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
        return (result[0], result[1]) if result else (None, 0)

    except Exception as e:
        logger.error(f"Error processing date {tweet_date}: {e}")
        raise

def q1_memory(file_path: str, num_threads: int = 1) -> List[Tuple[date, str]]:
    """Finds top 10 dates with most tweets and the user with highest tweets for each date."""
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