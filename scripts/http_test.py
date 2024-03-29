import requests
from pprint import pprint
import time
import threading

# pa's blog
# GATEWAY='http://hanbaoaaa.xyz/waverless_api1'

# deploy cluster
# GATEWAY='http://192.168.31.162:'

# deploy single node
GATEWAY='http://127.0.0.1:2501'

# APP='fn2'
# APP='longchain'
APP='word_count'

def run_one():
    ms = time.time()*1000.0
    res = requests.post(f'{GATEWAY}/{APP}')

    ms_ret = time.time()*1000.0
    print(res, ms_ret-ms,res.text)

# 10 concurrent requests by multi-threading
for i in range(1):
    threading.Thread(target=run_one).start()