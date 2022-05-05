from pylibs.staticdata.generator.cex_drivers.master_driver import Driver
import dateutil, pytz
import time, json, requests
from dateutil import parser
from datetime import datetime, timedelta
class IndexDriver(Driver):
    def __init__(self, bookname: str(), exchangeid: int()):
        super().__init__()
        self.index_name = bookname
        self.endpoint = ''
        self.method = ''
        self.payload = dict()
        self.index_components = []
        self.weights = []
        self.book_exchange_pairs = []
        self.ismapped = False
        self.map = dict()
        self.exchangeid = exchangeid
        #self.raw_data = self.load_raw_data()
    def load_raw_data(self):
        time.sleep(2)
        return json.loads(requests.get(f'{self.endpoint}/{self.method}', params=self.payload).content.decode('utf-8'))
    def get_index_components(self):
        return self.index_components
    def get_weights(self):
        return self.weights
    def get_book_exchange_pairs(self):
        return self.book_exchange_pairs
    def get_ismapped(self):
        return self.ismapped
    def get_latest_index_from_API(self)->list():
        api_index = []
        raw_data = self.load_raw_data()
        if raw_data is not None and 'error' not in raw_data and raw_data != []:
            time_reference = raw_data[0]['timestamp']
            for component in raw_data:
                if component['timestamp'] == time_reference and component['weight'] is not None:
                    api_index.append(component)
        return api_index
    def set_index_components(self, api_index:list()):
        for component in api_index:
            self.index_components.append({'exchange': component['reference'],
                                        'weight': component['weight'],
                                        'bookname': component['symbol']})
    def get_index_exchangeid(self, bitmex_name):
        return self.map[bitmex_name]
    def set_weights(self, index_components):
        for component in index_components:
            self.weights.append(component['weight'])

    def set_book_exchange_pairs(self, index_components):
        for component in index_components:
            self.book_exchange_pairs.append({'bookname': component['bookname'],
                                'exchangeid':self.get_index_exchangeid(component['exchange'])})
    def set_index(self):
        API = self.get_latest_index_from_API()
        if len(API) > 0:
            self.set_index_components(API)
            self.set_weights(self.get_index_components())
            self.set_book_exchange_pairs(self.get_index_components())
            self.ismapped = True
