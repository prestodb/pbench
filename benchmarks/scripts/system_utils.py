import paramiko
import sys

def execute_ssh_command(worker_ip, login_user, ssh_key_path, command):
    ssh = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        private_key = paramiko.Ed25519Key(filename=ssh_key_path)

        ssh.connect(hostname=worker_ip, username=login_user, pkey=private_key)

        stdin, stdout, stderr = ssh.exec_command(command)
        stdout_output = stdout.read().decode()
        stderr_output = stderr.read().decode()

        if stderr_output:
            print(f'Error on {worker_ip}: {stderr_output}')
            sys.exit(1)
        else:
            print(f'Successfully finished running command on {worker_ip}')
    except paramiko.SSHException as ssh_err:
        print(f'SSH error on {worker_ip}: {ssh_err}')
        sys.exit(1)
    except FileNotFoundError as key_err:
        print(f'SSH key file not found: {key_err}')
        sys.exit(1)
    except Exception as e:
        print(f'Failed to connect to {worker_ip}: {str(e)}')
        sys.exit(1)
    finally:
        if ssh:
            ssh.close()