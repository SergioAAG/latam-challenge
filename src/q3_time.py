import duckdb
import logging
from typing import List, Tuple

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def q3_time(file_path: str) -> List[Tuple[str, int]]:
    """Returns top 10 most mentioned users using time-optimized single query processing."""
    try:
        logger.info(f"Starting processing for file: {file_path}")

        with duckdb.connect(database=':memory:') as con:
            query = """
            SELECT 
                mentioned_user AS username,
                COUNT(*) AS mention_count
            FROM (
                SELECT 
                    UNNEST(mentionedUsers) AS mentioned_user
                FROM read_parquet(?)
                WHERE mentionedUsers IS NOT NULL
            )
            GROUP BY username
            ORDER BY mention_count DESC, username ASC
            LIMIT 10;
            """

            results = con.execute(query, [file_path]).fetchall()

        logger.info("Processing completed successfully")
        return results

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except duckdb.BinderException as e:
        logger.error(f"Error in file structure or query: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise
