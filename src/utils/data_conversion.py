import pyarrow as pa
import pyarrow.parquet as pq
import jsonlines
from datetime import datetime

def extract_relevant_data(file_path: str, output_parquet_path: str):
   """
   Extracts relevant data from a JSONL file and converts it to a Parquet file format.
   The function processes date, username, content and mentioned users from each JSON entry.

   Parameters
   ----------
   file_path : str
       Path to the input JSONL file.
   output_parquet_path : str
       Path where the output Parquet file will be saved.

   Returns
   -------
   None
       The function saves the Parquet file to the specified output path.

   Raises
   ------
   jsonlines.Error
       If there is an error reading the JSONL file.
   pa.lib.ArrowInvalid
       If there is an error creating the Arrow table or schema.
   OSError
       If there are file access or writing permission issues.
   ValueError
       If the date format in the JSONL file is invalid.
   TypeError
       If the data types in the JSONL file don't match the expected schema.
   """
   dates = []
   usernames = []
   contents = []
   mentioned_users = []

   with jsonlines.open(file_path) as reader:
       for obj in reader:
           date_str = obj.get('date')
           try:
               date = datetime.fromisoformat(date_str.replace('Z', '+00:00')) if date_str else None
           except (ValueError, TypeError):
               date = None
           dates.append(date)

           usernames.append(obj.get('user', {}).get('username', ''))
           contents.append(obj.get('content', ''))
           mentioned_users.append([user.get('username', '') for user in (obj.get('mentionedUsers') or [])])

   schema = pa.schema([
       ('date', pa.timestamp('ms')),
       ('username', pa.string()),
       ('content', pa.string()),
       ('mentionedUsers', pa.list_(pa.string()))
   ])

   table = pa.Table.from_arrays([pa.array(dates, type=pa.timestamp('ms')),
                                 pa.array(usernames, type=pa.string()),
                                 pa.array(contents, type=pa.string()),
                                 pa.array(mentioned_users, type=pa.list_(pa.string()))],
                                schema=schema)

   pq.write_table(table, output_parquet_path)
