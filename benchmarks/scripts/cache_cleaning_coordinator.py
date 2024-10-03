from presto_utils import execute_cluster_call
import argparse
import sys

def clean_directory_list_cache(hostname, username, password, catalog_name):
    query = "CALL " + catalog_name + ".system.invalidate_directory_list_cache()"
    return execute_cluster_call(hostname, username, password, catalog_name, query)

def clean_metastore_cache(hostname, username, password, catalog_name):
    query = "CALL " + catalog_name + ".system.invalidate_metastore_cache()"
    return execute_cluster_call(hostname, username, password, catalog_name, query)

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
        for catalog_name in catalog_list:
            print("Cleaning up directory list cache for ", catalog_name)
            rows = clean_directory_list_cache(args.host, args.username, args.password, catalog_name)
            print("directory_list_cache_cleanup_query Query Result: ", rows)
            if rows[0][0] == True:
                print("Directory list cache clean up is successful for ", catalog_name)
            else:
                print("Directory list cache clean up is failed for ", catalog_name)
                sys.exit(1)

    # Metadata cache clean up
    if is_metadata_cache_cleanup_enabled:
        for catalog_name in catalog_list:
            print("Cleaning up metadata cache for ", catalog_name)
            rows = clean_metastore_cache(args.host, args.username, args.password, catalog_name)
            print("metastore_cache_cleanup_query Query Result: ", rows)
            if rows[0][0] == True:
                print("Metastore cache clean up is successful for ", catalog_name)
            else:
                print("Metastore cache clean up is failed for ", catalog_name)
                sys.exit(1)
