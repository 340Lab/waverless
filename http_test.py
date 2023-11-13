import requests
from pprint import pprint

res = requests.post('http://localhost:3001/hahahah',
                    json={'after_which': 0,
                          'order_by': 0,
                          'tags': [],
                          'search_str': "",
                          'price_range': []}, verify=False)
pprint(res)
