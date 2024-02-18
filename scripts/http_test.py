import requests
from pprint import pprint
import time

ms = time.time()*1000.0
res = requests.get('http://127.0.0.1:2501/word_count')
#   json={'after_which': 0,
#         'order_by': 0,
#         'tags': [],
#         'search_str': "",
#         'price_range': []}, verify=False)
ms_ret = time.time()*1000.0
print(res, ms_ret-ms)
