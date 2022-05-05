from pylibs.staticdata.generator.cex_drivers.master_driver import Driver
from pylibs.staticdata.generator.book_field import BookField
from pylibs.staticdata.generator.book_type_map import BookTypes
import datetime, dateutil, pytz, json, requests
from pylibs.staticdata.generator.cex_drivers.pillar_driver import FundingPillars
from pylibs.staticdata.generator.cex_drivers.index_driver import IndexDriver

class FTXusDriver(Driver):

    def __init__(self):
        super().__init__()
        self.exchangeid = 39
        self.endpoint = 'https://ftx.us/api'
        self.method = 'markets'
        self.default_contract_size = int(1)
        self.default_unit_size = float(1)

    def get_books(self):
        return self.load_raw_data()['result']

    def get_base(self, book: dict()) -> str():
        if 'baseCurrency' in book and book['baseCurrency'] is not None:
            return book['baseCurrency']
        else:
            if '-' in self.get_bookname(book):
                return self.get_bookname(book).split('-')[0]
            elif '/' in self.get_bookname(book):
                return self.get_bookname(book).split('/')[0]
            else:
                return self.get_bookname(book)

    def get_quote(self, book: dict()) -> str():
        if 'quoteCurrency' in book and book['quoteCurrency'] is not None or '':
            return book['quoteCurrency']
        else:
            return 'USD'

    def format_date(self, date: str()):
        day = date[2:4]
        month = date[0:2]
        year = '20' + date[4:]
        return year + "-" + month + "-" + day + "T00:00:00.000Z"

    def get_exchange_type(self, book: dict()) ->str():
        if 'type' in book:
            return book['type']
        else:
            #print('!!NO TYPE: ',book['name'] )
            return None

    def get_bookname(self, book):
        return book['name']

    def get_proposed_book(self, book):
        proposed_book = super().get_proposed_book()
        proposed_book[BookField.FEE_INSTRUMENT.value] = self.get_base(book)
        proposed_book[BookField.BOOKNAME.value] = self.get_bookname(book)
        proposed_book[BookField.BASE.value] = self.get_base(book)
        proposed_book[BookField.QUOTE.value] = self.get_quote(book)
        proposed_book[BookField.TICK_SIZE.value] = float(book['priceIncrement'])
        proposed_book[BookField.QTY_INCREMENT.value] = float(book['sizeIncrement'])
        proposed_book[BookField.PRICE_PRECISION.value] = int(
            self.calculate_precision(book['priceIncrement']))
        proposed_book[BookField.QTY_PRECISION.value] = int(
            self.calculate_qty_precision(book['sizeIncrement']))
        proposed_book[BookField.ACTIVE.value] = True
        proposed_book[BookField.MARGIN_INSTRUMENT.value] = 'USD'
        proposed_book[BookField.SETTLEMENT_INSTRUMENT.value] = self.get_base(book)
        proposed_book[BookField.BOOK_TYPE.value] = self.get_exchange_type(book)
        return self.reformated(proposed_book)




