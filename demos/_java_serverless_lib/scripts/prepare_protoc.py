
### chdir
import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)


### utils
def os_system_sure(command):
    print(f"Run：{command}\n")
    result = os.system(command)
    if result != 0:
        print(f"\n  >>> Fail：{command}\n\n")
        exit(1)
    print(f"\n  >>> Succ：{command}\n\n")


# result.returncode
# result.stdout
def run_cmd_return(cmd):
    print(f"Run：{cmd}\n\n")
    result = subprocess.run(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    print(f"Stdout：{result.stdout}")
    
    return result

print("Proto Src Dir:")
os_system_sure("ls ../../../src/worker/func/shared/")
print("\n\n")

print("Proto target Dir:")
os_system_sure("ls ../core/src/main/java/io/serverless_lib/")
print("\n\n"  )

os_system_sure("protoc --proto_path=../../../src/ \
--java_out=../core/src/main/java/io/serverless_lib ../../../src/worker/func/shared/process_rpc_proto.proto")
