from presto_utils import execute_presto_query
import argparse
import sys

def clean_directory_list_cache(hostname, username, password, catalog_name):
    query = "CALL " + catalog_name + ".system.invalidate_directory_list_cache()"
    return execute_presto_query(hostname, username, password, catalog_name, query)

def clean_metastore_cache(hostname, username, password, catalog_name):
    query = "CALL " + catalog_name + ".system.invalidate_metastore_cache()"
    return execute_presto_query(hostname, username, password, catalog_name, query)

def clean_statistics_file_cache(hostname, username, password, catalog_name):
    query = "CALL " + catalog_name + ".system.invalidate_statistics_file_cache()"
    return execute_presto_query(hostname, username, password, catalog_name, query)

def clean_manifest_file_cache(hostname, username, password, catalog_name):
    query = "CALL " + catalog_name + ".system.invalidate_manifest_file_cache()"
    return execute_presto_query(hostname, username, password, catalog_name, query)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to PrestoDB')
    parser.add_argument('--host', required=True, help='Hostname of the Presto coordinator')
    parser.add_argument('--username', required=True, help='Username to connect to Presto')
    parser.add_argument('--password', required=True, help='Password to connect to Presto')

    args = parser.parse_args()

    hive_catalog_list = ["hive"]
    iceberg_catalog_list = ["iceberg"]

    # coordinator cache
    is_list_cache_cleanup_enabled = True  #for hive
    is_metadata_cache_cleanup_enabled = True #for hive
    is_statistics_file_cache_cleanup_enabled = True #for iceberg
    is_manifest_file_cache_cleanup_enabled = True #for iceberg

    # Directory list cache clean up
    if is_list_cache_cleanup_enabled:
        for catalog_name in hive_catalog_list:
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
        for catalog_name in hive_catalog_list:
            print("Cleaning up metadata cache for ", catalog_name)
            rows = clean_metastore_cache(args.host, args.username, args.password, catalog_name)
            print("metastore_cache_cleanup_query Query Result: ", rows)
            if rows[0][0] == True:
                print("Metastore cache clean up is successful for ", catalog_name)
            else:
                print("Metastore cache clean up is failed for ", catalog_name)
                sys.exit(1)

    # Statistics file cache clean up
    if is_statistics_file_cache_cleanup_enabled:
        for catalog_name in iceberg_catalog_list:
            print("Cleaning up statistics file cache for ", catalog_name)
            rows = clean_statistics_file_cache(args.host, args.username, args.password, catalog_name)
            print("statistics_file_cache_cleanup_query Query Result: ", rows)
            if rows[0][0] == True:
                print("Statistics file cache clean up is successful for ", catalog_name)
            else:
                print("Statistics file cache clean up is failed for ", catalog_name)
                sys.exit(1)

    # Manifest file cache clean up
    if is_manifest_file_cache_cleanup_enabled:
        for catalog_name in iceberg_catalog_list:
            print("Cleaning up manifest file cache for ", catalog_name)
            rows = clean_manifest_file_cache(args.host, args.username, args.password, catalog_name)
            print("manifest_file_cache_cleanup_query Query Result: ", rows)
            if rows[0][0] == True:
                print("Manifest file cache clean up is successful for ", catalog_name)
            else:
                print("Manifest file cache clean up is failed for ", catalog_name)
                sys.exit(1)
