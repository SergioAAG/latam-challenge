import duckdb
import logging
from typing import List, Tuple
from collections import Counter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def get_flattened_mentions(con: duckdb.DuckDBPyConnection, file_path: str) -> None:
    """Creates a temporary view of flattened user mentions."""
    query = f"""
    CREATE TEMP VIEW flattened_mentions AS
    SELECT 
        UNNEST(mentionedUsers) AS username
    FROM read_parquet('{file_path}')
    WHERE mentionedUsers IS NOT NULL;
    """
    con.execute(query)

def get_mention_counts(con: duckdb.DuckDBPyConnection) -> List[Tuple[str, int]]:
    """Gets top 10 users by mention count."""
    query = """
    SELECT 
        username,
        COUNT(*) AS mention_count
    FROM flattened_mentions
    GROUP BY username
    ORDER BY mention_count DESC, username ASC
    LIMIT 10;
    """
    return con.execute(query).fetchall()

def q3_memory(file_path: str) -> List[Tuple[str, int]]:
    """Returns top 10 most mentioned users using memory-optimized processing."""
    try:
        logger.info(f"Starting processing for file: {file_path}")

        with duckdb.connect(database=':memory:') as con:            
            get_flattened_mentions(con, file_path)
            results = get_mention_counts(con)

            mention_counts = Counter({username: count for username, count in results})
            top_mentions = mention_counts.most_common(10)

            logger.info("Processing completed successfully")
            return top_mentions

    except FileNotFoundError:
        logger.error(f"File not found: {file_path}")
        raise
    except duckdb.BinderException as e:
        logger.error(f"Error in file structure or query: {e}")
        raise
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise
