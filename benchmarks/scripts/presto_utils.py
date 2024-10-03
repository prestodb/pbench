import prestodb

# Establish Presto connection
def create_connection(hostname, username, password, catalog_name):
    conn = prestodb.dbapi.connect(
        host=hostname,
        port=443,
        user=username,
        catalog=catalog_name,
        schema='',
        http_scheme='https',
        auth=prestodb.auth.BasicAuthentication(username, password)
    )
    return conn

def execute_presto_query(hostname, username, password, cluster_name, query):
    conn = create_connection(hostname, username, password, cluster_name)
    try:
        cur = conn.cursor()
        cur.execute(query)
        rows = cur.fetchall()
    finally:
        conn.close()
    return rows