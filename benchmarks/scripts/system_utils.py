import paramiko
import sys

def execute_ssh_command(worker_ip, login_user, ssh_key_path, command):
    ssh = None
    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        private_key = paramiko.Ed25519Key(filename=ssh_key_path)

        ssh.connect(hostname=worker_ip, username=login_user, pkey=private_key, timeout=30)

        stdin, stdout, stderr = ssh.exec_command(command)
        stdout_output = stdout.read().decode()
        stderr_output = stderr.read().decode()

        exit_status = stdout.channel.recv_exit_status()
        if exit_status != 0:
            if stderr_output:
                print(f'Error on {worker_ip} (exit status {exit_status}): {stderr_output}')
            else:
                print(f'Error on {worker_ip}: command exited with status {exit_status}')
            sys.exit(1)
        else:
            if stderr_output:
                print(f'Command on {worker_ip} completed with warnings: {stderr_output}')
            print(f'Successfully finished running command on {worker_ip}')
            return stdout_output
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