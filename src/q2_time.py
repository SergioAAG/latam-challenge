import pyarrow.parquet as pq
from collections import Counter
import emoji
from typing import List, Tuple
import logging
from concurrent.futures import ProcessPoolExecutor
import multiprocessing
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def extract_emojis_from_content(content: str) -> List[str]:
    """
    Extracts emojis from a string of text.

    Parameters
    ----------
    content : str
        Tweet content to extract emojis from.

    Returns
    -------
    List[str]
        A list of emojis found in the content.
    """
    if not isinstance(content, str):
        return []
    return [d['emoji'] for d in emoji.emoji_list(content)] if content else []

def process_chunk(contents: List[str]) -> Counter:
    """
    Processes a chunk of tweet contents and returns an emoji counter.

    Parameters
    ----------
    contents : List[str]
        List of tweet contents to process.

    Returns
    -------
    Counter
        A Counter object with the counts of each emoji.
    """
    chunk_counter = Counter()
    for content in contents:
        if content:
            chunk_counter.update(extract_emojis_from_content(content))
    return chunk_counter

def q2_time(file_path: str) -> List[Tuple[str, int]]:
    """
    Returns the top 10 most used emojis and their respective counts.
    This version prioritizes execution speed by loading the entire file into memory.

    Parameters
    ----------
    file_path : str
        Path to the Parquet file.

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
    ValueError
        If the file is empty or has incorrect formatting.
    RuntimeError
        For unexpected errors during processing.
    """
    logger.info(f"Starting time-optimized processing for file: {file_path}")

    if not isinstance(file_path, str):
        raise TypeError("File path must be a string")

    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found: {file_path}")

    try:
        table = pq.read_table(file_path, columns=['content'])

        if table.num_rows == 0:
            logger.warning(f"File is empty: {file_path}")
            return []

        contents = table['content'].to_pandas().astype(str).tolist()

        num_cpus = multiprocessing.cpu_count()
        num_processes = min(num_cpus * 2, 16)

        chunk_size = max(1, len(contents) // num_processes)
        chunks = [contents[i:i + chunk_size] for i in range(0, len(contents), chunk_size)]

        with ProcessPoolExecutor(max_workers=num_processes) as executor:
            counters = list(executor.map(process_chunk, chunks))

        final_counter = Counter()
        for counter in counters:
            final_counter.update(counter)

        result = final_counter.most_common(10)

        for emoji_char, count in result:
            if not isinstance(emoji_char, str) or not isinstance(count, int):
                raise ValueError("Invalid data types in result")

        logger.info("Processing completed successfully")
        return result

    except pq.lib.ArrowInvalid:
        logger.error(f"Invalid Parquet file: {file_path}")
        raise ValueError("Invalid or corrupted Parquet file")
    except Exception as e:
        logger.error(f"Error during processing: {e}")
        raise RuntimeError(f"Unexpected error: {e}")
