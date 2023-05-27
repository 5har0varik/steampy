import requests
import json
from itertools import cycle
import os


class Proxy:
    def __init__(self, json_filename):
        self.proxy_list = []
        self.json_file = json_filename
        if os.path.exists(self.json_file):
            f = open(self.json_file, "r")
            self.proxy_data = json.loads(f.read())
            response = requests.get(self.proxy_data["Url"], headers=self.proxy_data["Headers"])
            print(response.json())
            for item in response.json()["results"]:
                #https='socks5://user:pass@host:port'
                if item['valid']:
                    self.proxy_list.append({"https": "socks5://" + item['username'] + ':' + item['password'] + '@' + item['proxy_address'] + ':' + str(item['port'])})
        self.proxy_list_iter = cycle(self.proxy_list)
        self.address_calls = 0
        self.address_calls_max = 1
        self.current_proxy = next(self.proxy_list_iter)

    def get_proxy(self):
        if self.address_calls < self.address_calls_max:
            self.address_calls += 1
        else:
            self.current_proxy = next(self.proxy_list_iter)
            self.address_calls = 1
        print(self.address_calls)
        return self.current_proxy
