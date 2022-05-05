from pathlib import Path
from pylibs.staticdata.generator.cex_drivers.master_driver import Driver
from pylibs.staticdata.generator.book_field import BookField
from pylibs.staticdata.generator.book_type_map import BookTypes
import datetime, dateutil, pytz, json, requests
from pylibs.staticdata.generator.cex_drivers.pillar_driver import FundingPillars
from pylibs.staticdata.generator.cex_drivers.index_driver import IndexDriver
import os


class FTXDriver(Driver):
    def __init__(self):
        super().__init__()
        self.exchangeid = 23
        self.endpoint = 'https://ftx.com/api'
        self.method = 'markets'
        self.method2 = 'futures'
        self.method3 = 'wallet/coins'
        self.default_contract_size = int(1)
        self.default_unit_size = float(1)
        self.markets = self.get_markets()

    def get_markets(self):
        data = self.load_raw_data()['result']
        return data

    def get_indices(self):
        indices = list()
        for book in self.markets: # self.load_raw_data()['result']:
            if self.get_exchange_type(book) == 'future':
                indices.append(self.build_index(book['underlying'], book))
        return indices

    def get_futures(self):
        data = self.load_raw_data(self.method2)['result']
        return data

    def get_non_futures(self):
        books = list()
        for book in self.markets:
            if book['type'] != 'future':
                books.append(book)
        return books

    def get_coin_wallets(self):
        coin_data = self.load_raw_data(self.method3)['result']
        return coin_data

    def is_token(self, book):
        boolean = False
        for market in self.markets:
            if market['name'] == book['name']:
                if 'tokenizedEquity' in market:
                    boolean = market['tokenizedEquity']
        return boolean

    def post_mapping_cleaning(self, book):
        return book

    def get_books(self):
        books = list()
        books += self.get_indices()
        books += self.get_futures()
        books += self.get_non_futures()
        books += self.get_coin_wallets()
        return books

    def build_index(self, index_name: str(), book:dict()) -> dict():
        index_book = dict()
        index_book['name'] = index_name
        index_book[BookField.BOOKNAME.value] = index_name
        index_book[BookField.BASE.value] = index_name
        index_book[BookField.QUOTE.value] = 'USD'
        index_book[BookField.BOOK_TYPE.value] = BookTypes.INDEX.value
        index_book[BookField.INDEX.value] = dict()
        index_book[BookField.ACTIVE.value] = True
        index_book[BookField.TRADABLE.value] = False
        return index_book

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

    def get_collateral_weight(self, book):
        pass

    def get_min_order_qty(self, book):
        if 'minProvideSize' in book:
            return book['minProvideSize']

    def get_proposed_book(self, book):
        proposed_book = super().get_proposed_book()
        if 'collateralWeight' in book.keys():
            # This is to get all the loan cash bookids.
            # All books must have a book_type
            # ETFs are ignored
            if book['isEtf'] is False:
                proposed_book[BookField.BOOKNAME.value] = self.get_bookname(book)
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.LOAN.value
                proposed_book[BookField.LOAN_INSTRUMENT.value] = book['id']
                # No spotMargin means you cannot borrow
                proposed_book[BookField.CAN_BORROW.value] = True if book['spotMargin'] == 0 else False
                proposed_book[BookField.COLLATERAL_WEIGHT.value] = book['collateralWeight']
                proposed_book[BookField.COLLATERAL_WEIGHT_INITIAL.value] = max(0.01, book['collateralWeight'])
            return proposed_book
        elif 'type' not in book.keys():   # For index
            proposed_book.update(book)
            del proposed_book['name']
            return proposed_book
        else:
            proposed_book = dict()
            proposed_book[BookField.FEE_INSTRUMENT.value] = self.get_quote(book)
            proposed_book[BookField.BOOKNAME.value] = self.get_bookname(book)
            proposed_book[BookField.BASE.value] = self.get_base(book)
            proposed_book[BookField.QUOTE.value] = self.get_quote(book)
            proposed_book[BookField.TICK_SIZE.value] = float(book['priceIncrement'])
            proposed_book[BookField.QTY_INCREMENT.value] = float(book['sizeIncrement'])
            proposed_book[BookField.PRICE_PRECISION.value] = int(self.calculate_precision(book['priceIncrement']))
            proposed_book[BookField.QTY_PRECISION.value] = int(self.calculate_qty_precision(book['sizeIncrement']))
            proposed_book[BookField.ACTIVE.value] = True
            proposed_book[BookField.MARGIN_INSTRUMENT.value] = 'USD'
            proposed_book[BookField.SETTLEMENT_INSTRUMENT.value] = self.get_base(book)
            proposed_book[BookField.BOOK_TYPE.value] = self.get_exchange_type(book)
            if self.get_min_order_qty(book):
                proposed_book[BookField.MIN_ORDER_QTY.value] = self.get_min_order_qty(book)

            if self.get_exchange_type(book) != 'spot':
                proposed_book[BookField.CONTRACT_SIZE.value] = self.default_contract_size
                proposed_book[BookField.UNIT_SIZE.value] = self.default_unit_size
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.LINEAR_FUTURE.value
                proposed_book[BookField.UNIT_INSTRUMENT.value] = self.get_base(book)
                proposed_book[BookField.STD_MULTIPLIER.value] = self.default_contract_size
                proposed_book[BookField.UNDERLYING_BOOKNAME.value] = book['underlying']
                proposed_book[BookField.UNDERLYING_EXCHANGEID.value] = self.exchangeid
                proposed_book[BookField.IMFFACTOR.value] = book['imfFactor']
                if self.get_exchange_type(book) == 'future':
                    proposed_book[BookField.EXPIRY.value] = book['expiry']
                    proposed_book[BookField.SETTLE.value] = book['expiry']
                if self.get_exchange_type(book) == 'perpetual':
                    bf = FTXFundingPillars(self.get_bookname(book), self.exchangeid)
                    proposed_book[BookField.FUNDING_PILLARS.value] = bf.get_funding_pillars()
                    proposed_book[BookField.BOOK_TYPE.value] = BookTypes.LINEAR_SWAP.value
                if self.get_exchange_type(book) in ['move', 'prediction']:
                    proposed_book[BookField.BOOK_TYPE.value] = BookTypes.VOLATILITY_DERIVATIVE.value
                if self.get_bookname(book) == 'PERP/USD':
                    proposed_book['active'] = False
            if self.is_token(book):
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.TOKENIZED_ASSET.value
            return self.reformated(proposed_book)

class FTXFundingPillars(FundingPillars):
    def __init__(self, bookname, exchangeid):
        self.endpoint = 'v2/public/funding/prev-funding-rate'
        self.daily_pillars_nb = 24
        self.utc_hours = [ele*24/self.daily_pillars_nb for ele in range(0, self.daily_pillars_nb)]
        self.funding_pillars = [int(ele*3600) for ele in self.utc_hours]

class FTXIndex(IndexDriver):
    def __init__(self, bookname, exchangeid):
        super(FTXIndex, self).__init__(bookname, exchangeid)
        self.endpoint = 'https://ftx.com/api'
        self.method = f'indexes/{bookname}/weights'
        # self.payload = {'symbol':bookname, 'count': "10", "reverse": "true"}

    def load_raw_data(self):
        time.sleep(2)
        return json.loads(requests.get(f'{self.endpoint}/{self.method}').content.decode('utf-8'))





