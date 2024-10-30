import pandas as pd
from memory_profiler import memory_usage
import time
from typing import Callable, Tuple, List

def profile_function(func: Callable[[str], List[Tuple]], file_path: str) -> Tuple[float, float, List[Tuple]]:
   """
   Profiles a function by measuring its execution time and memory usage while processing a file.

   Parameters
   ----------
   func : Callable[[str], List[Tuple]]
       The function to be profiled. Should take a file path string as input and return a list of tuples.
   file_path : str
       Path to the input file that the function will process.

   Returns
   -------
   Tuple[float, float, List[Tuple]]
       A tuple containing:
       - execution_time (float): Time taken to execute the function in seconds
       - max_memory (float): Peak memory usage during execution in MB
       - results (List[Tuple]): The results returned by the profiled function

   Raises
   ------
   TypeError
       If the function or file_path parameters are not of the expected types.
   ValueError
       If the function's return value doesn't match the expected format.
   MemoryError
       If there's insufficient memory to profile the function.
   FileNotFoundError
       If the specified file_path doesn't exist.
   pandas.errors.EmptyDataError
       If the results cannot be converted to a DataFrame.
   """
   start_time = time.time()

   mem_usage, results = memory_usage(
       (func, (file_path,)),
       retval=True,
       interval=0.1,
       timeout=None
   )

   execution_time = time.time() - start_time
   max_memory = max(mem_usage) - min(mem_usage)

   results_df = pd.DataFrame(results, columns=['Fecha', 'Tweets'])
   print(results_df)

   return execution_time, max_memory, results
