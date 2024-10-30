import pyarrow.parquet as pq
import emoji
from collections import Counter
from typing import List, Tuple, Generator, Iterator
import logging
from contextlib import contextmanager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@contextmanager
def create_parquet_iterator(file_path: str, batch_size: int = 10000) -> Iterator:
    """
    Creates an efficient memory iterator for reading Parquet files in batches.

    Parameters
    ----------
    file_path : str
        Path to the Parquet file.
    batch_size : int, optional
        Number of rows to read per batch (default is 1000).

    Yields
    ------
    Iterator
        Iterator over the records in the Parquet file in batches.

    Raises
    ------
    Exception
        If the file cannot be opened or read.
    """
    try:
        parquet_file = pq.ParquetFile(file_path)
        yield parquet_file.iter_batches(batch_size=batch_size, columns=['content'])
    except Exception as e:
        logger.error(f"Error opening the Parquet file: {e}")
        raise

def extract_emojis(content: str) -> Generator[str, None, None]:
    """
    Extracts emojis from tweet content using a generator.

    Parameters
    ----------
    content : str
        Tweet content.

    Yields
    ------
    str
        Each emoji found in the content.
    """
    if content and isinstance(content, str):
        for emoji_dict in emoji.emoji_list(content):
            yield emoji_dict['emoji']

def update_counter_from_batch(batch, emoji_counter: Counter) -> None:
    """
    Updates the emoji counter with a batch of Parquet records.

    Parameters
    ----------
    batch : RecordBatch
        Batch of records from the Parquet file.
    emoji_counter : Counter
        Counter to update with extracted emojis.
    """
    for content in batch['content']:
        if content is not None:
            content_str = str(content.as_py())
            emoji_counter.update(extract_emojis(content_str))

def q2_memory(file_path: str, batch_size: int = 10000) -> List[Tuple[str, int]]:
    """
    Returns the top 10 most used emojis and their respective counts, optimized for memory usage.

    Parameters
    ----------
    file_path : str
        Path to the Parquet file.
    batch_size : int, optional
        Number of rows to process per batch (default is 1000).

    Returns
    -------
    List[Tuple[str, int]]
        A list of tuples containing:
            - Emoji (str)
            - Count (int)

    Raises
    ------
    FileNotFoundError
        If the specified file does not exist.
    Exception
        If an unexpected error occurs during processing.
    """
    logger.info(f"Starting processing for file: {file_path}")
    emoji_counter = Counter()
    processed_rows = 0

    try:
        with create_parquet_iterator(file_path, batch_size) as iterator:
            for batch in iterator:
                update_counter_from_batch(batch, emoji_counter)
                processed_rows += batch.num_rows

                if processed_rows % (batch_size * 10) == 0:
                    logger.info(f"Processed {processed_rows:,} records")

        logger.info(f"Processing completed. Total records: {processed_rows:,}")
        return emoji_counter.most_common(10)

    except Exception as e:
        logger.error(f"Error processing file: {e}")
        raise
