### chdir
import os
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)


### utils
def os_system_sure(command):
    print(f"执行命令：{command}")
    result = os.system(command)
    if result != 0:
        print(f"命令执行失败：{command}")
        exit(1)
    print(f"命令执行成功：{command}\n\n")

def os_system(command):
    print(f"执行命令：{command}")
    result = os.system(command)
    print("\n\n")


os.system("git clone https://github.com/ActivePeter/feishu2everywhere")

os.chdir("feishu2everywhere")


# with open("try.py", "r") as f:
#     script = f.read()

# script=script.replace(
#     'driver.get("https://fvd360f8oos.feishu.cn/docx/Q3c6dJG5Go3ov6xXofZcGp43nfb")',
#     'driver.get("https://fvd360f8oos.feishu.cn/docx/XSxcdONk2oVJD5xtZuicxftqn3f")',
# )

# with open("try.py", "w") as f:
#     f.write(script)
#     f.close()

# os_system_sure("python3 try.py")

os_system_sure("mv canvas*.png ../../figs/")

with open("out.md", "r") as f:
    rdme = f.read()
rdme=rdme.replace(
    '](canvas',
    '](figs/canvas',
)
with open("out.md", "w") as f:
    f.write(rdme)
    f.close()

os_system_sure("cp out.md ../../README.md")