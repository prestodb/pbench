from presto_utils import create_connection
import argparse
import sys

def clean_directory_list_cache(hostname, username, password, catalogName):
    query = "CALL " + catalogName + ".system.invalidate_directory_list_cache()"
    conn = create_connection(hostname, username, password, catalogName)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

def clean_metastore_cache(hostname, username, password, catalogName):
    query = "CALL " + catalogName + ".system.invalidate_metastore_cache()"
    conn = create_connection(hostname, username, password, catalogName)
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    conn.close()
    return rows

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to PrestoDB')
    parser.add_argument('--host', required=True, help='Hostname of the Presto coordinator')
    parser.add_argument('--username', required=True, help='Username to connect to Presto')
    parser.add_argument('--password', required=True, help='Password to connect to Presto')

    args = parser.parse_args()

    catalog_list = ["hive"]
    is_list_cache_cleanup_enabled = True
    is_metadata_cache_cleanup_enabled = False

    # Directory list cache clean up
    if is_list_cache_cleanup_enabled:
        for catalogName in catalog_list:
            print("Cleaning up directory list cache for", catalogName)
            rows = clean_directory_list_cache(args.host, args.username, args.password, catalogName)
            print("directory_list_cache_cleanup_query Query Result:", rows)
            if rows[0][0] == True:
                print("Directory list cache clean up is successfull for", catalogName)
            else:
                sys.exit(1)
                print("Directory list cache clean up is failed for", catalogName)

    # Metadata cache clean up
    if is_metadata_cache_cleanup_enabled:
        for catalogName in catalog_list:
            print("Cleaning up metadata cache for", catalogName)
            rows = clean_metastore_cache(args.host, args.username, args.password, catalogName)
            print("metastore_cache_cleanup_query Query Result:", rows)
            if rows[0][0] == True:
                print("Metastore cache clean up is successfull for", catalogName)
            else:
                sys.exit(1)
                print("Metastore cache clean up is failed for", catalogName)
