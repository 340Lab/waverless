import requests
from pprint import pprint
import time

ms = time.time()*1000.0
res = requests.get('http://192.168.31.199:3001/fn2')
#   json={'after_which': 0,
#         'order_by': 0,
#         'tags': [],
#         'search_str': "",
#         'price_range': []}, verify=False)
ms_ret = time.time()*1000.0
print(res, ms_ret-ms)
