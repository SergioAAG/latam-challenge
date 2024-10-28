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
    """Creates an iterator for reading Parquet files in batches."""
    try:
        parquet_file = pq.ParquetFile(file_path)
        yield parquet_file.iter_batches(batch_size=batch_size, columns=['content'])
    except Exception as e:
        logger.error(f"Error opening the Parquet file: {e}")
        raise

def extract_emojis(content: str) -> Generator[str, None, None]:
    """Extracts emojis from tweet content."""
    if content and isinstance(content, str):
        for emoji_dict in emoji.emoji_list(content):
            yield emoji_dict['emoji']

def update_counter_from_batch(batch, emoji_counter: Counter) -> None:
    """Updates emoji counter with emojis from a batch of records."""
    for content in batch['content']:
        if content is not None:
            content_str = str(content.as_py())
            emoji_counter.update(extract_emojis(content_str))

def q2_memory(file_path: str, batch_size: int = 10000) -> List[Tuple[str, int]]:
    """Returns top 10 most used emojis and their counts, optimized for memory usage."""
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
