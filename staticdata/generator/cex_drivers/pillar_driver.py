from pylibs.staticdata.generator.cex_drivers.master_driver import Driver
import dateutil, pytz
import time, json, requests
from dateutil import parser
from datetime import datetime, timedelta
class FundingPillars(Driver):
    def __init__(self, bookname : str(), exchangeid : int()):
        super().__init__()
        self.endpoint = ''
        self.method =''
        self.daily_pillars_nb = 0
        self.now = datetime.utcnow()
        self.bookname = bookname
        self.payload = dict()
        self.funding_pillars = list()
        self.exchangeid = exchangeid

    def get_funding_pillars(self)->list():
        if len(self.funding_pillars) > 1:
            return sorted(self.funding_pillars)
        else:
            return self.funding_pillars

    def get_pillar_daily_seconds(self, funding_timestamp):
        pillar_time = parser.isoparse(funding_timestamp)
        return pillar_time.hour*60*60 + pillar_time.minute*60 + pillar_time.second

    def load_raw_data(self):
        time.sleep(2)
        return json.loads(requests.get(f'{self.endpoint}/{self.method}', params=self.payload).content.decode('utf-8'))

    def parse_funding_pillars(self):
        raw_data = self.load_raw_data()
        if len(raw_data)>= self.daily_pillars_nb and 'error' not in raw_data :
            for funding_info in raw_data:
                if 'timestamp' in funding_info and funding_info['timestamp'] is not None:
                    self.funding_pillars.append(self.get_pillar_daily_seconds(funding_info['timestamp']))
                else:
                    print('no pillar retrieved', self.bookname)
        else:
            print('no pillar retrieved', self.bookname)
