import pandas as pd
import sqlite3 
conn = sqlite3.connect("inventory.db") 

def optimize_sqlite(conn):
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA cache_size=-100000")

def create_table_from_df(conn, table_name, df, unique_cols=None):
    dtype_map = {
        "int64": "INTEGER",
        "float64": "REAL",
        "object": "TEXT",
        "bool": "INTEGER",
    }

    columns_sql = []

    for col, dtype in df.dtypes.items():
        sql_type = dtype_map.get(str(dtype), "TEXT")
        columns_sql.append(f'"{col}" {sql_type}')

    if unique_cols:
        quoted = [f'"{c}"' for c in unique_cols]
        columns_sql.append(f"UNIQUE ({', '.join(quoted)})")

    create_sql = f"""
    CREATE TABLE IF NOT EXISTS "{table_name}" (
        {', '.join(columns_sql)}
    );
    """

    conn.execute(create_sql)
    conn.commit()


def insert_dataframe_sqlite(conn,df,table_name,chunk_size,if_exists="IGNORE"):
    cursor = conn.cursor()
    cols=[]
    for c in df.columns:
        cols.append(f'"{c}"')
    cols_names=",".join(cols)
    placeholders=','.join(['?']*len(cols) )
    
    insert_sql = f"""
                INSERT OR {if_exists.upper()} INTO {table_name} 
                ({cols_names}) 
                VALUES ({placeholders})
            """
    total = len(df)
    for start in range(0,total,chunk_size):
        end = min(start+chunk_size,total)
        chunk = df.iloc[start:end]

        data = list( chunk.itertuples(name=None,index=False) )
        conn.execute("BEGIN TRANSACTION")
        cursor.executemany(insert_sql,data)
        conn.commit()

        #print(f"inserted rows{start} -> {end} ")

        
#function calls start 
def optimize_insertion_code(df,table_name,engine):
    optimize_sqlite(conn) 
    create_table_from_df(conn,table_name,df)
    insert_dataframe_sqlite(conn,df,table_name,chunk_size=100000,if_exists="ignore")
