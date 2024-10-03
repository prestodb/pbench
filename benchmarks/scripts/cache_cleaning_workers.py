from mysql_utils import create_connection
from mysql_utils import execute_mysql_query
import json
import paramiko
import argparse
import sys

def get_workers_public_ips(data):
    data = json.loads(data)

    worker_ips = []
    for key, value in data["output"].items():
        if key.startswith("swarm_presto_worker") and key.endswith("public_ip"):
            worker_ips.append(value)
    return worker_ips

def cleanup_worker_disk_cache(worker_public_ips, directory_to_cleanup, login_user, ssh_key_path):
    for worker_ip in worker_public_ips:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            private_key = paramiko.Ed25519Key(filename=ssh_key_path)

            ssh.connect(hostname=worker_ip, username=login_user, pkey=private_key)

            cleanup_command = f'sudo rm -rf {directory_to_cleanup}/*'
            stdin, stdout, stderr = ssh.exec_command(cleanup_command)
            stdout_output = stdout.read().decode()
            stderr_output = stderr.read().decode()

            if stderr_output:
                print(f'Error on {worker_ip}: {stderr_output}')
            else:
                print(f'Successfully cleaned up {directory_to_cleanup} on {worker_ip}')
        except Exception as e:
            print(f'Failed to connect to {worker_ip}: {str(e)}')
        finally:
            ssh.close()
            sys.exit(1)

def cleanup_worker_os_cache(worker_public_ips, login_user, ssh_key_path):
    for worker_ip in worker_public_ips:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            private_key = paramiko.Ed25519Key(filename=ssh_key_path)

            ssh.connect(hostname=worker_ip, username=login_user, pkey=private_key)

            os_cache_clean_commands = ["sudo sync && echo 3 | sudo tee /proc/sys/vm/drop_caches", "sudo swapoff -a; sudo swapon -a"]
            for command in os_cache_clean_commands :
                stdin, stdout, stderr = ssh.exec_command(command)
                stdout_output = stdout.read().decode()
                stderr_output = stderr.read().decode()

                if stderr_output:
                    print(f'Error on {worker_ip}: {stderr_output}')
                else:
                    print(f'Successfully ran command : {command} on {worker_ip}')

        except Exception as e:
            print(f'Failed to connect to {worker_ip}: {str(e)}')
        finally:
            ssh.close()
            sys.exit(1)

# Main function to connect and run queries
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Connect to PrestoDB')
    parser.add_argument('--mysql', required=True, help='Mysql database details')
    parser.add_argument('--clustername', required=True, help='Presto cluster name')
    parser.add_argument('--sshkey', required=True, help='SSH key to connect to Presto Vms')

    args = parser.parse_args()

    with open(args.mysql, 'r') as file:
        mysqlDetails = json.load(file)
    username = mysqlDetails.get("username")
    password = mysqlDetails.get("password")
    server = mysqlDetails.get("server")
    database = mysqlDetails.get("database")

    connection = create_connection(
        host_name=server,
        user_name=username,
        user_password=password,
        db_name=database
    )

    clusterName = args.clustername
    select_query = "SELECT detail_json FROM presto_clusters WHERE cluster_name = %s"
    results = execute_mysql_query(connection, select_query, clusterName)

    worker_public_ips = []
    if results:
        worker_public_ips = get_workers_public_ips(results[0][0])

    print("worker count = ", len(worker_public_ips))
    print("======= worker_public_ips ======")
    print(worker_public_ips)

    is_worker_disk_cache_cleanup_enabled = True
    is_worker_os_cache_cleanup_enabled = True

    if is_worker_disk_cache_cleanup_enabled:
        native_cache_directory_worker = "/home/centos/presto/async_data_cache"
        cleanup_worker_disk_cache(worker_public_ips, native_cache_directory_worker, "centos", args.sshkey)

    if is_worker_os_cache_cleanup_enabled:
        cleanup_worker_os_cache(worker_public_ips, "centos", args.sshkey)

    if connection.is_connected():
        connection.close()
        print("The connection is closed.")
