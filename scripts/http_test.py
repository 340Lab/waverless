import requests
from pprint import pprint
import time
import threading
import os
import subprocess
CUR_FPATH = os.path.abspath(__file__)
CUR_FDIR = os.path.dirname(CUR_FPATH)
# chdir to the directory of this script
os.chdir(CUR_FDIR)


# pa's blog
# GATEWAY='http://hanbaoaaa.xyz/waverless_api1'

# deploy cluster
# GATEWAY='http://192.168.31.162:'

# deploy single node
GATEWAY='http://127.0.0.1:2606'

# FUNC='fn2/fn2'

# APP='longchain'
# APP='word_count'

def run_one():
    ms = time.time()*1000.0
    
    # res = requests.post(f'{GATEWAY}/fn2/fn2')
    res = requests.post(f'{GATEWAY}/stock-mng/createUser', json={
        "userName": "test"})

    ms_ret = time.time()*1000.0
    print(res, ms_ret-ms,res.text)

# run_one()

# valid when testing with deploy_single_node
def upload_app(appname,rename):
    appdir=f"deploy_single_node/pack/waverless_backend/test_dir1/apps/{appname}"
    os.chdir(appdir)
    
    entries=os.listdir(f"./")
    entries_concat=" ".join(entries)
    os.system(f"zip -r {rename}.zip {entries_concat}")
    os.system(f"mv {rename}.zip {CUR_FDIR}")
    os.chdir(CUR_FDIR)
    
    filepath=f"{rename}.zip"
    files=[]
    f= open(filepath, 'rb')
    files.append((rename, (filepath.split('/')[-1], f, 'application/zip')))
    
    try:
        response = requests.post(f'{GATEWAY}/appmgmt/upload_app', files=files)
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
    # time.sleep(10);
    # print(response.status_code, response.text)

upload_app("stock-mng","testapp")