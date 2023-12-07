import subprocess
import sys
import os

def is_command_installed(command):
    try:
        subprocess.check_call([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False
    except FileNotFoundError:
        return False

def install_command(installation_command, prerequisites):
    for command in prerequisites:
        if not is_command_installed(command):
            print(f"Installing {command} as a prerequisite...")
            # run installation_command
            os.system(installation_command)
        else:
            print(f"{command} is already installed.")

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python install_if_no.py <install_command> <prerequisite1> <prerequisite2> ...")
        sys.exit(1)

    installation_command = sys.argv[1]
    prerequisites = sys.argv[2:]
    
    install_command(installation_command, prerequisites)
