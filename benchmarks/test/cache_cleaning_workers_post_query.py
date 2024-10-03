import mysql.connector
from mysql.connector import Error
import json
import paramiko

def create_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(
            host=host_name,
            user=user_name,
            passwd=user_password,
            database=db_name
        )
        print("Connection to Benchmark Database is successful")
    except Error as e:
        print(f"The error '{e}' occurred")

    return connection

def execute_select_query(connection, query, clusterName):
    cursor = connection.cursor()
    try:
        cursor.execute(query, (clusterName,))
        result = cursor.fetchall()
        return result
    except Error as e:
        print(f"The error '{e}' occurred")

def get_workers_public_ips(data):
    data = json.loads(data)

    worker_ips = []
    for key, value in data["output"].items():
        if key.startswith("swarm_presto_worker") and key.endswith("public_ip"):
            worker_ips.append(value)
    return worker_ips

def cleanup_worker_cache(worker_public_ips, directory_to_cleanup, login_user, ssh_key_path):
    for worker_ip in worker_public_ips:
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            private_key = paramiko.Ed25519Key(filename=ssh_key_path)

            ssh.connect(hostname=worker_ip, username=login_user, pkey=private_key)

            cleanup_command = f'rm -rf {directory_to_cleanup}/*'
            stdin, stdout, stderr = ssh.exec_command(cleanup_command)
            stdout_output = stdout.read().decode()
            stderr_output = stderr.read().decode()

            if stderr_output:
                print(f'Error on {ip}: {stderr_output}')
            else:
                print(f'Successfully cleaned up {directory_to_cleanup} on {worker_ip}')
        except Exception as e:
            print(f'Failed to connect to {worker_ip}: {str(e)}')
        finally:
            ssh.close()

# Main function to connect and run queries
if __name__ == "__main__":
    connection = create_connection(
        host_name="presto-deploy-mysql3ff6596.cmtorbpxxkla.us-east-1.rds.amazonaws.com",
        user_name="presto",
        user_password="1IqLIyBVFUfWK5dSEZFhBcuXcS9EbKAkq4d5d",
        db_name="presto_benchmarks"
    )

    clusterName = "xsmall-0.290-20240915200031-9aacb0c-b2492n-reetika-eng";
    select_query = "SELECT detail_json FROM presto_clusters WHERE cluster_name = %s"
    results = execute_select_query(connection, select_query, clusterName)

    worker_public_ips = []
    if results:
        worker_public_ips = get_workers_public_ips(results[0][0])

    print("worker count = ", len(worker_public_ips))
    print("======= worker_public_ips ======")
    print(worker_public_ips)

    cleanup_worker_cache(worker_public_ips, "/home/centos/temp/", "centos", "/Users/reetikaagrawal/.ssh/id_ed25519_ibm_presto_deploy_cluster")

    if connection.is_connected():
        connection.close()
        print("The connection is closed.")
