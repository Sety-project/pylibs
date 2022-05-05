import urllib3.util.connection as urllib3_cn
import requests, json
import pandas as pd
import datetime, dateutil, pytz
from pylibs.staticdata.generator.fees_table import fees_map
from pylibs.staticdata.generator.book_field import BookField
from pylibs.staticdata.generator.book_type_map import book_types_map, BookTypes, instrument_uniform_naming_map
import os

def allowed_gai_family():
    '''
        Function to force the requests package to use the Ipv4 protocol
        Loads when the master Driver is loaded
        Solves long query time when calling an endpoint that tries first Ipv6 protocol and
        wait it times out before trying the Ipv4 one
        :return: ipv4 socket
    '''
    import socket

    family = socket.AF_INET
    return family

urllib3_cn.allowed_gai_family = allowed_gai_family

class Driver:
    def __init__(self):
        self.endpoint = ''
        self.method = ''
        self.field_mapping = {}
        self.books = []
        self.exchangeid = None

    def get_proposed_book(self):
        proposed_book = {}
        proposed_book[BookField.EXCHANGEID.value] = self.exchangeid
        return proposed_book

    def get_rawdata(self, book):
        return book

    def get_exchangeid(self):
        return self.exchangeid

    def post_mapping_cleaning(self, book):
        return book

    def load_raw_data(self, other_method=''):
        if other_method:
            return json.loads(requests.get(f'{self.endpoint}/{other_method}').content.decode('utf-8'))
        else:
            return json.loads(requests.get(f'{self.endpoint}/{self.method}').content.decode('utf-8'))

    def query_raw_data(self, query:str()):
        return requests.post(f'{self.endpoint}/{self.method}', json={"query":query}).json()

    def get_books(self):
        data = self.load_raw_data()
        for b in data:
            book = {}
            for f in self.field_mapping.keys():
                book[f] = b[self.field_mapping[f]]
            book = self.post_mapping_cleaning(book)
            self.books.append(book)
        return self.books

    def get_book_type(self, book:dict())->str():
        return book.get(BookField.BOOK_TYPE.value, None)

    def round_scientific(self, scientific_value):
        if 'e-' in str(scientific_value):
            return float(("%.17f" % float(scientific_value)).rstrip('0').rstrip('.'))
        elif scientific_value is not None:
            return float(scientific_value)
        else:
            return None

    def clean_scientific(self, book:dict()):
        for field, value in book.items():
            if type(value) is float() and value is not None:
                book[field] = self.round_scientific(value)
        return book

    def get_exchange_type(self, book: dict())->str():
        return None

    def get_book_type_id(self, book_type: str())->int():
        return int(book_types_map[book_type])

    def get_taker_fees(self):
        for exchange in fees_map:
            if exchange['exchangeid'] == self.exchangeid:
                if 'taker' in exchange:
                    return float(format(round(float(exchange['taker']*100),4),".4f"))

    def get_maker_fees(self):
        for exchange in fees_map:
            if exchange['exchangeid'] == self.exchangeid:
                if 'maker' in exchange:
                    return float(format(round(float(exchange['maker']*100),4), ".4f"))

    def get_fees(self, proposed_book) :
        if BookField.ACTIVE_FEE_BPS.value not in proposed_book:
            proposed_book[BookField.ACTIVE_FEE_BPS.value] = self.get_taker_fees()
        if BookField.PASSIVE_FEE_BPS.value not in proposed_book:
            proposed_book[BookField.PASSIVE_FEE_BPS.value] = self.get_maker_fees()
        proposed_book[BookField.ACTIVE_FEE_CST.value] = float(0)
        proposed_book[BookField.PASSIVE_FEE_CST.value] = float(0)
        return proposed_book

    def singular(self,word):
        if str(word)[-1] == 's':
            return str(word)[:-1]
        else:
            return word

    def get_bookname(self, book):
        return None
    def df_view(self):
        return pd.DataFrame.from_dict(self.get_books())

    def calculate_precision(self, precision):
        if len(str(precision)) == 1:
            return int(precision)
        elif 'e-' in str(precision):
            price_dec = ("%.17f" % float(precision)).rstrip('0').rstrip('.')
            return len(str(price_dec).split('.')[1])
        elif '.' in str(precision):
            if str(precision).split('.')[0] == '0':
                return len(str(precision).split('.')[1])
            else:
                return 0
    def calculate_qty_precision(self, precision):
        if precision >= 1:
            return 0
        else:
            if 'e-' in str(precision):
                price_dec = ("%.17f" % float(precision)).rstrip('0').rstrip('.')
                return len(str(price_dec).split('.')[1])
            elif '.' in str(precision):
                if str(precision).split('.')[0] == '0':
                    return len(str(precision).split('.')[1])

    def contains_digit(self,string):
        for character in string:
            if character.isdigit():
                return True
        return False

    def reformated(self,proposed_book):
        if proposed_book.get(BookField.PRICE_PRECISION.value, None) is not None:
            proposed_book[BookField.PRICE_PRECISION.value] = int(proposed_book[BookField.PRICE_PRECISION.value])
        else:
            print('No price precision!!!', proposed_book[BookField.BOOKNAME.value], self.exchangeid)
            proposed_book[BookField.ACTIVE.value] = False

        if proposed_book.get(BookField.QTY_PRECISION.value, None) is not None:
             proposed_book[BookField.QTY_PRECISION.value] = int(proposed_book[BookField.QTY_PRECISION.value])
        else:
            print('No qty precision!!!', proposed_book[BookField.BOOKNAME.value], self.exchangeid)
            proposed_book[BookField.ACTIVE.value] = False

        if proposed_book.get(BookField.TICK_SIZE.value, None) is not None:
            proposed_book[BookField.TICK_SIZE.value] = float(proposed_book[BookField.TICK_SIZE.value])
        else:
            print('No tick size!!!', proposed_book[BookField.BOOKNAME], self.exchangeid)
            proposed_book[BookField.ACTIVE.value] = False

        if proposed_book.get(BookField.STD_MULTIPLIER.value, None) is not None:
            proposed_book[BookField.STD_MULTIPLIER.value] = int(proposed_book[BookField.STD_MULTIPLIER.value])

        if proposed_book.get(BookField.CONTRACT_SIZE.value, None) is not None:
            proposed_book[BookField.CONTRACT_SIZE.value] = int(proposed_book[BookField.CONTRACT_SIZE.value])

        if proposed_book.get(BookField.UNIT_SIZE.value, None) is not None:
            proposed_book[BookField.UNIT_SIZE.value] = float(proposed_book[BookField.UNIT_SIZE.value])

        if proposed_book.get(BookField.ACTIVE_FEE_BPS.value, None) is not None:
            proposed_book[BookField.ACTIVE_FEE_BPS.value] = float(round(float(proposed_book[BookField.ACTIVE_FEE_BPS]), 4))

        if proposed_book.get(BookField.PASSIVE_FEE_BPS.value, None) is not None:
            proposed_book[BookField.PASSIVE_FEE_BPS.value] = float(round(float(proposed_book[BookField.PASSIVE_FEE_BPS]), 4))

        if proposed_book.get(BookField.EXPIRY.value, None) is not None:
            proposed_book[BookField.EXPIRY.value] = self.date_cleaning(proposed_book[BookField.EXPIRY.value])

        if proposed_book.get(BookField.SETTLE.value, None) is not None:
            proposed_book[BookField.SETTLE.value] = self.date_cleaning(proposed_book[BookField.SETTLE.value])

        if proposed_book.get(BookField.INITIAL_MARGIN.value, None) is None:
            proposed_book[BookField.INITIAL_MARGIN.value] = float(0);

        if proposed_book.get(BookField.MAINTENANCE_MARGIN.value, None) is None:
            proposed_book[BookField.MAINTENANCE_MARGIN.value] = float(0);

        proposed_book = self.clean_scientific(proposed_book)
        proposed_book[BookField.TRADABLE.value] = False
        if proposed_book.get(BookField.BOOK_TYPE.value, None) == BookTypes.SPOT.value:
            proposed_book[BookField.UNIT_INSTRUMENT.value] = proposed_book.get(BookField.BASE.value, None)
        proposed_book = self.instrument_uninaming(proposed_book)
        return self.get_fees(proposed_book)

    def date_cleaning(self, date:str())->str():
        if 'Z' not in date:
            date += 'Z'
        if '+' in date:
            date = date.replace('+', ':')
        if "00:00:00:00Z" in date:
            date = date.replace("00:00:00:00Z", "00:00.000Z")
        if ":00Z" in date:
            date = date.replace(":00Z", ".000Z")
        return date

    def instrument_uninaming(self, proposed_book):
        if BookField.BASE.value in proposed_book and proposed_book[BookField.BASE.value] is not None:
            proposed_book[BookField.BASE.value] = str(proposed_book[BookField.BASE.value]).upper()
            if proposed_book[BookField.BASE.value] in instrument_uniform_naming_map.keys():
                proposed_book[BookField.BASE.value] = instrument_uniform_naming_map[proposed_book[BookField.BASE.value]]
        else:
            print('NO BASE !!!! ', proposed_book)

        if BookField.QUOTE.value in proposed_book and proposed_book[BookField.QUOTE.value] is not None:
            proposed_book[BookField.QUOTE.value] = str(proposed_book[BookField.QUOTE.value]).upper()
            if proposed_book[BookField.QUOTE.value] in instrument_uniform_naming_map.keys():
                proposed_book[BookField.QUOTE.value] = instrument_uniform_naming_map[proposed_book[BookField.QUOTE.value]]
        else:
            print('NO QUOTE !!!! ', proposed_book)

        if BookField.UNIT_INSTRUMENT.value in proposed_book and proposed_book[BookField.UNIT_INSTRUMENT.value] is not None:
            proposed_book[BookField.UNIT_INSTRUMENT.value] = str(proposed_book[BookField.UNIT_INSTRUMENT.value]).upper()
            if proposed_book[BookField.UNIT_INSTRUMENT.value] in instrument_uniform_naming_map.keys():
                proposed_book[BookField.UNIT_INSTRUMENT.value] = instrument_uniform_naming_map[proposed_book[BookField.UNIT_INSTRUMENT.value]]

        if BookField.SETTLEMENT_INSTRUMENT.value in proposed_book and proposed_book[BookField.SETTLEMENT_INSTRUMENT.value] is not None:
            proposed_book[BookField.SETTLEMENT_INSTRUMENT.value] = str(proposed_book[BookField.SETTLEMENT_INSTRUMENT.value]).upper()
            if proposed_book[BookField.SETTLEMENT_INSTRUMENT.value] in instrument_uniform_naming_map.keys():
                proposed_book[BookField.SETTLEMENT_INSTRUMENT.value] = instrument_uniform_naming_map[
                    proposed_book[BookField.SETTLEMENT_INSTRUMENT.value]]

        if BookField.MARGIN_INSTRUMENT.value in proposed_book and proposed_book[BookField.MARGIN_INSTRUMENT.value] is not None:
            proposed_book[BookField.MARGIN_INSTRUMENT.value] = str(proposed_book[BookField.MARGIN_INSTRUMENT.value]).upper()
            if proposed_book[BookField.MARGIN_INSTRUMENT.value] in instrument_uniform_naming_map.keys():
                proposed_book[BookField.MARGIN_INSTRUMENT.value] = instrument_uniform_naming_map[
                    proposed_book[BookField.MARGIN_INSTRUMENT.value]]

        if BookField.FEE_INSTRUMENT.value in proposed_book and proposed_book[BookField.FEE_INSTRUMENT.value] is not None:
            proposed_book[BookField.FEE_INSTRUMENT.value] = str(proposed_book[BookField.FEE_INSTRUMENT.value]).upper()
            if proposed_book[BookField.FEE_INSTRUMENT.value] in instrument_uniform_naming_map.keys():
                proposed_book[BookField.FEE_INSTRUMENT.value] = instrument_uniform_naming_map[
                    proposed_book[BookField.FEE_INSTRUMENT.value]]

        if proposed_book.get(BookField.INITIAL_MARGIN.value, None) is None:
            proposed_book[BookField.INITIAL_MARGIN.value] = float(0);

        if proposed_book.get(BookField.MAINTENANCE_MARGIN.value, None) is None:
            proposed_book[BookField.MAINTENANCE_MARGIN.value] = float(0);

        return proposed_book


