import requests
from pprint import pprint
import time
import threading

def run_one():
    ms = time.time()*1000.0
    res = requests.post('http://hanbaoaaa.xyz/waverless_api1/panote_auth/verify_token',json={"token": "hhhh"})
    #   json={'after_which': 0,
    #         'order_by': 0,
    #         'tags': [],
    #         'search_str': "",
    #         'price_range': []}, verify=False)
    ms_ret = time.time()*1000.0
    print(res, ms_ret-ms,res.text)

# 10 concurrent requests by multi-threading
for i in range(1):
    threading.Thread(target=run_one).start()