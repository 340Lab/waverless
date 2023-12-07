import subprocess
import sys

def is_command_installed(command):
    try:
        # 使用subprocess.check_call来执行指令，如果指令存在则返回0，否则抛出异常
        subprocess.check_call([command], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        return True
    except subprocess.CalledProcessError:
        return False
    except FileNotFoundError:
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <command>")
        sys.exit(1)

    command_to_check = sys.argv[1]
    result = is_command_installed(command_to_check)

    # 通过echo输出结果
    if result:
        print("true")
    else:
        print("false")