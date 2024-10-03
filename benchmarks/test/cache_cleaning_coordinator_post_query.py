import prestodb
import argparse

# Establish Presto connection
def create_connection(hostname, password):
    conn = prestodb.dbapi.connect(
        host=hostname,
        port=443,
        user='presto',
        catalog='hive',
        schema='',
        http_scheme='https',
        auth=prestodb.auth.BasicAuthentication('presto', password)
    )
    return conn

def execute_select_query(query, hostname, password):
    conn = create_connection(hostname, password)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

def clean_directory_list_cache(catalogName, hostname, password):
    query = "CALL " + catalogName + ".system.invalidate_directory_list_cache()"
    conn = create_connection(hostname, password)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to PrestoDB')
    parser.add_argument('--host', required=True, help='Hostname of the Presto coordinator')
    parser.add_argument('--password', required=True, help='Password of the Presto coordinator')

    args = parser.parse_args()

    select_query = 'SELECT * FROM system.runtime.nodes'
    rows = execute_select_query(select_query, args.host, args.password)
    print("select_query Query Result:", rows)

    # Directory list cache clean up
    # catalogName = "hive"
    # rows = clean_directory_list_cache(catalogName, args.host, args.password)
    # print("directory_list_cache_cleanup_query Query Result:", rows)
    # if rows[0][0] == True :
    #     print("Directory list cache clean up is successfull!")
    # else :
    #     print("Directory list cache clean up is failed!")
