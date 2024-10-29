import pandas as pd
import cProfile
from memory_profiler import memory_usage
import time
from typing import Callable, Tuple, List

def profile_function(func: Callable[[str], List[Tuple]], file_path: str) -> Tuple[float, float, List[Tuple]]:
    """
    Profiles a function by measuring execution time and memory usage.
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

