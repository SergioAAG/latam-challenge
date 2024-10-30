import matplotlib.pyplot as plt
from typing import List

def plot_profile_results(function_names: List[str], execution_times: List[float], memory_usages: List[float]):
   """
   Generates comparative bar graphs showing execution time and memory usage for different functions.

   Parameters
   ----------
   function_names : List[str]
       List of function names to be displayed on the x-axis of both graphs.
   execution_times : List[float]
       List of execution times in seconds corresponding to each function.
   memory_usages : List[float]
       List of memory usage values in megabytes corresponding to each function.

   Returns
   -------
   None
       Displays the matplotlib figure with two bar graphs side by side.

   Raises
   ------
   ValueError
       If the lengths of function_names, execution_times, and memory_usages lists don't match.
   TypeError
       If the input parameters are not of the expected types.
   matplotlib.error
       If there are issues creating or displaying the plots.
   """
   fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12, 5))

   ax1.bar(function_names, execution_times, color='tab:red')
   ax1.set_title('Tiempo de Ejecución')
   ax1.set_xlabel('Función')
   ax1.set_ylabel('Tiempo (s)')

   ax2.bar(function_names, memory_usages, color='tab:blue')
   ax2.set_title('Uso de Memoria')
   ax2.set_xlabel('Función')
   ax2.set_ylabel('Memoria (MB)')

   plt.suptitle('Comparación de Tiempo de Ejecución y Uso de Memoria')

   plt.tight_layout(rect=[0, 0, 1, 0.95])
   plt.show()
