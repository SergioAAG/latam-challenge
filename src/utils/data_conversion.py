import pyarrow as pa
import pyarrow.parquet as pq
import jsonlines
from datetime import datetime

def extract_relevant_data(file_path: str, output_parquet_path: str):
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

