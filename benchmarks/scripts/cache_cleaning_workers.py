from mysql_utils import create_connection
from mysql_utils import execute_mysql_query
from system_utils import execute_ssh_command
import json
import argparse
import sys

def get_workers_public_ips(data):
    data = json.loads(data)

    worker_ips = []
    for key, value in data["output"].items():
        if key.startswith("swarm_presto_worker") and key.endswith("public_ip"):
            worker_ips.append(value)
    return worker_ips

def build_cache_clean_command(worker_http_port, cache_type, container_name_pattern):
    return (
        f"docker exec $(docker ps -q --filter 'name={container_name_pattern}') "
        f"curl -sS -X GET http://localhost:{worker_http_port}/v1/operation/server/clearCache?type={cache_type}"
    )

def cleanup_worker_disk_cache(worker_public_ips, login_user, ssh_key_path, worker_http_port, container_name_pattern):
    ssd_cache_clean_command = build_cache_clean_command(worker_http_port, "ssd", container_name_pattern)
    for worker_ip in worker_public_ips:
        execute_ssh_command(worker_ip, login_user, ssh_key_path, ssd_cache_clean_command)
    print("cleanup_worker_disk_cache is successful!")

def cleanup_worker_os_cache(worker_public_ips, login_user, ssh_key_path):
    for worker_ip in worker_public_ips:
        os_cache_clean_commands = ["sudo sync && echo 3 | sudo tee /proc/sys/vm/drop_caches", "sudo swapoff -a && sudo swapon -a"]
        for command in os_cache_clean_commands:
            execute_ssh_command(worker_ip, login_user, ssh_key_path, command)
    print("cleanup_worker_os_cache is successful!")

def cleanup_worker_memory_cache(worker_public_ips, login_user, ssh_key_path, worker_http_port, container_name_pattern):
    memory_cache_clean_command = build_cache_clean_command(worker_http_port, "memory", container_name_pattern)
    for worker_ip in worker_public_ips:
        execute_ssh_command(worker_ip, login_user, ssh_key_path, memory_cache_clean_command)
    print("cleanup_worker_memory_cache is successful!")

# Main function to connect and run queries
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to PrestoDB')
    parser.add_argument('--mysql', required=True, help='Mysql database details')
    parser.add_argument('--clustername', required=True, help='Presto cluster name')
    parser.add_argument('--sshkey', required=True, help='SSH key to connect to Presto Vms')
    parser.add_argument('--sshuser', default='centos', help='SSH login user for Presto VMs')
    parser.add_argument('--worker-http-port', default='8080', help='Presto worker HTTP port (default: 8080)')
    parser.add_argument('--container-name-pattern', default='^presto_workers', help='Docker container name filter pattern')
    parser.add_argument('--cleanup-disk-cache', action='store_true', help='Cleanup worker disk cache only when specified')
    parser.add_argument('--cleanup-os-cache', action='store_true', help='Cleanup worker OS cache only when specified')
    parser.add_argument('--cleanup-memory-cache', action='store_true', help='Cleanup worker memory cache only when specified')

    args = parser.parse_args()

    with open(args.mysql, 'r') as file:
        mysql_details = json.load(file)
    username = mysql_details.get("username")
    password = mysql_details.get("password")
    server = mysql_details.get("server")
    database = mysql_details.get("database")

    connection = create_connection(
        host_name=server,
        user_name=username,
        user_password=password,
        db_name=database
    )

    cluster_name = args.clustername
    select_query = "SELECT detail_json FROM presto_clusters WHERE cluster_name = %s"
    results = execute_mysql_query(connection, select_query, cluster_name)

    if not results:
        print(f"No cluster details found for cluster '{cluster_name}'. Exiting without cleanup.")
        if connection.is_connected():
            connection.close()
        sys.exit(1)

    worker_public_ips = get_workers_public_ips(results[0][0])
    if not worker_public_ips:
        print(f"No worker public IPs found for cluster '{cluster_name}'. Exiting without cleanup.")
        if connection.is_connected():
            connection.close()
        sys.exit(1)

    print("worker count = ", len(worker_public_ips))
    print("======= worker_public_ips ======")
    print(worker_public_ips)

    selected_any_cleanup = (
        args.cleanup_disk_cache or
        args.cleanup_os_cache or
        args.cleanup_memory_cache
    )
    if selected_any_cleanup:
        is_worker_disk_cache_cleanup_enabled = args.cleanup_disk_cache
        is_worker_os_cache_cleanup_enabled = args.cleanup_os_cache
        is_worker_memory_cache_cleanup_enabled = args.cleanup_memory_cache
    else:
        is_worker_disk_cache_cleanup_enabled = True
        is_worker_os_cache_cleanup_enabled = True
        is_worker_memory_cache_cleanup_enabled = True

    worker_http_port = args.worker_http_port
    ssh_login_user = args.sshuser
    container_name_pattern = args.container_name_pattern
    if is_worker_disk_cache_cleanup_enabled:
        cleanup_worker_disk_cache(worker_public_ips, ssh_login_user, args.sshkey, worker_http_port, container_name_pattern)

    if is_worker_memory_cache_cleanup_enabled:
        cleanup_worker_memory_cache(worker_public_ips, ssh_login_user, args.sshkey, worker_http_port, container_name_pattern)

    if is_worker_os_cache_cleanup_enabled:
        cleanup_worker_os_cache(worker_public_ips, ssh_login_user, args.sshkey)

    if connection.is_connected():
        connection.close()
        print("The connection is closed.")
