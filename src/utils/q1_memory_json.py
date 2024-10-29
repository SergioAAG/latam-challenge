import duckdb
import datetime
from typing import List, Tuple

def q1_memory_json(file_path: str) -> List[Tuple[datetime.date, str]]:
    con = duckdb.connect(database=':memory:')

    con.execute("""
    CREATE TABLE IF NOT EXISTS tweets (
        tweet_date DATE,
        username VARCHAR
    );
    """)

    try:

        offset = 0
        chunk_size = 200000 
        while True:
            query = f"""
            INSERT INTO tweets
            SELECT
                date_trunc('day', CAST(date AS TIMESTAMP))::DATE AS tweet_date,
                user->>'username' AS username
            FROM read_json_auto('{file_path}', maximum_object_size=1000000)
            LIMIT {chunk_size} OFFSET {offset};
            """
            con.execute(query)

            if con.execute("SELECT COUNT(*) FROM tweets;").fetchone()[0] < offset + chunk_size:
                break
            offset += chunk_size


        top_dates = con.execute("""
        SELECT
            tweet_date,
            COUNT(*) AS tweet_count
        FROM tweets
        GROUP BY tweet_date
        ORDER BY tweet_count DESC
        LIMIT 10;
        """).fetchall()

        results = []
        for row in top_dates:
            tweet_date = row[0]
            top_user = con.execute(f"""
            SELECT
                username,
                COUNT(*) AS user_tweet_count
            FROM tweets
            WHERE tweet_date = '{tweet_date}'
            GROUP BY username
            ORDER BY user_tweet_count DESC
            LIMIT 1;
            """).fetchone()
            results.append((tweet_date, top_user[0]))

    except duckdb.BinderException as e:

        con.close()
        return []
    except Exception as e:
        con.close()
        raise e

    con.close()
    return results