import pandas as pd

def execute_query(conn, query, params=None):
    try:
        with conn.cursor() as cur:
            cur.execute(query, params)
            affected_rows = cur.rowcount 

            if cur.description: 
                result = cur.fetchall()
                return result
            else:
                conn.commit()
                return affected_rows  
    except Exception as e:
        print('There was an error when trying to execute the query.')
        raise e

def query_to_df(conn, query):
    try:
        with conn.cursor() as cur:
            cur.execute(query)
            
            if cur.description is not None:
                # Get column names
                colnames = [desc[0] for desc in cur.description]
                # Fetch all rows
                rows = cur.fetchall()
                # Return as Pandas DataFrame
                return pd.DataFrame(rows, columns=colnames)
            else:
                print('Query did not return any rows')
                return pd.DataFrame()
    except Exception as e:
        raise e