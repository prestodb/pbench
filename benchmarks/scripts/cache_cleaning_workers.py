from mysql_utils import create_connection
from mysql_utils import execute_mysql_query
from system_utils import execute_ssh_command
import json
import argparse

def get_workers_public_ips(data):
    data = json.loads(data)

    worker_ips = []
    for key, value in data["output"].items():
        if key.startswith("swarm_presto_worker") and key.endswith("public_ip"):
            worker_ips.append(value)
    return worker_ips

def cleanup_worker_disk_cache(worker_public_ips, login_user, ssh_key_path, worker_http_port):
    ssd_cache_clean_commands = ["curl", "-X", "GET", f"http://localhost:{worker_http_port}/v1/operation/server/clearCache?type=ssd"]
    for worker_ip in worker_public_ips:
        execute_ssh_command(worker_ip, login_user, ssh_key_path, ssd_cache_clean_commands)

def cleanup_worker_os_cache(worker_public_ips, login_user, ssh_key_path):
    for worker_ip in worker_public_ips:
        os_cache_clean_commands = ["sudo sync && echo 3 | sudo tee /proc/sys/vm/drop_caches", "sudo swapoff -a; sudo swapon -a"]
        for command in os_cache_clean_commands:
            execute_ssh_command(worker_ip, login_user, ssh_key_path, command)

def cleanup_worker_memory_cache(worker_public_ips, login_user, ssh_key_path, worker_http_port):
    for worker_ip in worker_public_ips:
        memory_cache_clean_commands = ["curl", "-X", "GET", f"http://localhost:{worker_http_port}/v1/operation/server/clearCache?type=memory"]
        for command in memory_cache_clean_commands:
            execute_ssh_command(worker_ip, login_user, ssh_key_path, command)

# Main function to connect and run queries
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to PrestoDB')
    parser.add_argument('--mysql', required=True, help='Mysql database details')
    parser.add_argument('--clustername', required=True, help='Presto cluster name')
    parser.add_argument('--sshkey', required=True, help='SSH key to connect to Presto Vms')

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

    worker_public_ips = []
    if results:
        worker_public_ips = get_workers_public_ips(results[0][0])

    print("worker count = ", len(worker_public_ips))
    print("======= worker_public_ips ======")
    print(worker_public_ips)

    is_worker_disk_cache_cleanup_enabled = True
    is_worker_os_cache_cleanup_enabled = True
    is_worker_memory_cache_cleanup_enabled = True

    worker_http_port = "8080"
    if is_worker_disk_cache_cleanup_enabled:
        cleanup_worker_disk_cache(worker_public_ips, "centos", args.sshkey, worker_http_port)

    if is_worker_memory_cache_cleanup_enabled:
        cleanup_worker_memory_cache(worker_public_ips, "centos", args.sshkey, worker_http_port)

    if is_worker_os_cache_cleanup_enabled:
        cleanup_worker_os_cache(worker_public_ips, "centos", args.sshkey)

    if connection.is_connected():
        connection.close()
        print("The connection is closed.")
