import prestodb
import argparse

# Establish Presto connection
def create_connection(hostname, username, password, catalogName):
    conn = prestodb.dbapi.connect(
        host=hostname,
        port=443,
        user=username,
        catalog=catalogName,
        schema='',
        http_scheme='https',
        auth=prestodb.auth.BasicAuthentication(username, password)
    )
    return conn

def execute_presto_query(query, hostname, username, password, catalogName):
    try:
        conn = create_connection(hostname, username, password, catalogName)
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
        conn.close()
        return rows
    except Exception as e:
        print(f"An error occurred while executing the query: {e}")
        if conn:
            conn.close()
