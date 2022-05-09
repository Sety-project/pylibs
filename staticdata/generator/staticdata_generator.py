import json  
import time 
from pylibs.staticdata.generator.cex_drivers.ccxt_public import CcxtPublic 
from pylibs.staticdata.generator.new_map import active_exchange_map 
from pylibs.staticdata.generator.book_field import BookField 
from pylibs.staticdata.generator.book_type_map import book_types_map, BookTypes
import collections 
import glob 
import os.path 
from pathlib import Path 

class StaticDataGenerator: 

    def __init__(self):
        self.drivers = []
        self.reverse_instruments = {}
        self.reverse_symbols = {} 
        self.log_name = "" 
        self.check_log_name = "" 
        self.file_name = "" 
        self.check_file_name = ""
        self.log_content = []

        # Maps
        self.books = []
        self.exchanges = []
        self.instruments = []
        self.symbols = []

        # By id
        self.static_books_by_id = {}
        self.symbols_by_id = {}
        self.instruments_by_id = {}
        self.exchange_map_by_id = {}

        # By name
        self.symbols_by_name = {}
        self.instruments_by_name = {}
        self.exchange_map_by_name = {}

        # Load FULL exchange maps
        self.full_exchange_map = []
        self.full_exchange_map_by_name = {}
        self.full_exchange_map_by_id = {}

        # Load full exchange map
        self.load_new_map()
        self.set_static_paths()

    def set_static_paths(self):
        home = str(Path.home())
        self.staticdata_path = os.path.join(home, 'static', 'staticdata')
        self.staticchecker_path = os.path.join(home, 'static', 'staticchecker')
        self.staticdata_logs_path = os.path.join(home, 'static', 'staticdata', 'Logs')
        self.staticdatachecker_logs_path = os.path.join(home, 'static', 'staticchecker', 'Logs')

    def get_file_name(self):
        time_date_stamp = time.strftime("%Y%m%d_%H%M%S")
        self.file_name = os.path.join(self.staticdata_path, 'static_file_' + time_date_stamp + '.json')
        self.check_file_name = os.path.join(self.staticchecker_path, 'static_file_by_id_' + time_date_stamp + '.json')
        return self.file_name

    def get_log_name(self):
        time_date_stamp = time.strftime("%Y%m%d_%H%M%S")
        self.log_name = os.path.join(self.staticdata_logs_path, 'log_from_static_' + time_date_stamp + '.txt')
        self.check_log_name = os.path.join(self.staticdatachecker_logs_path, 'log_from_static_' + time_date_stamp + '.txt')
        return self.log_name

    def load_all_statics(self, staticData): 
        # Loads from a previous static
        self.load_books(staticData)
        self.load_symbols(staticData)
        self.load_instruments(staticData)
        self.load_exchanges(staticData)

    def load_static_from_file(self, filepath):
        try:
            staticData = json.load(open(filepath, "r"))
        except:
            print("Could not load the previous static !")
            staticData = {'books': {}, 'exchanges': {}, 'instruments': {}, 'symbols': {}}
        self.load_all_statics(staticData)

    def load_books(self, static_data):
        books = static_data['books']
        if books:
            self.books = books
            self.set_static_books_by_id(books)

    def load_symbols(self, static_data):
        symbols = static_data['symbols']
        if symbols:
            self.symbols = symbols
            self.set_symbols_by_id(symbols)
            self.set_symbols_by_name(symbols)

    def load_instruments(self, static_data):
        instruments = static_data['instruments']
        if instruments:
            self.instruments = instruments
            self.set_instruments_by_id(instruments)
            self.set_instruments_by_name(instruments)

    def load_exchanges(self, static_data):
        exchanges = static_data['exchanges']
        if exchanges:
            self.exchanges = exchanges
            self.set_exchange_map_by_id(exchanges)
            self.set_exchange_map_by_name(exchanges)

    def set_static_books_by_id(self, books):
        self.static_books_by_id = {int(book['bookid']): book for book in books}

    def set_symbols_by_id(self, symbols):
        self.symbols_by_id = {int(s['symbolid']): s['symbolname'] for s in symbols}

    def set_symbols_by_name(self, symbols):
        self.symbols_by_name = {s['symbolname']: int(s['symbolid']) for s in symbols}

    def set_instruments_by_id(self, instruments):
        self.instruments_by_id = {int(i['instrumentid']): i['instrumentname'] for i in instruments}

    def set_instruments_by_name(self, instruments):
        self.instruments_by_name={i['instrumentname']: int(i['instrumentid']) for i in instruments}

    def set_exchange_map_by_id(self, exchanges):
        self.exchange_map_by_id = {int(e['exchangeid']): e['exchange'] for e in exchanges}

    def set_exchange_map_by_name(self, exchanges):
        self.exchange_map_by_name = {e['exchange']: int(e['exchangeid']) for e in exchanges}

    # def clean_symbols(self, symbols: list())->list():
    #     symbols_new = list()
    #     for symbol in symbols:
    #         symbol_new = dict()
    #         if 'symbolname' in symbol:
    #             symbol_new['symbol'] = symbol['symbolname']
    #         else:
    #             symbol_new['symbol'] = symbol['symbol']
    #         symbol_new['_id'] = symbol['_id']
    #         symbol_new['symbolid'] = symbol['symbolid']
    #         symbols_new.append(symbol_new)
    #     return symbols_new
    #
    # def clean_instruments(self, instruments: list()) -> list():
    #     instruments_new = list()
    #     for instrument in instruments:
    #         instrument_new = dict()
    #         if 'instrument_name' in instrument:
    #             instrument_new['instrument'] = instrument['instrumentname']
    #         elif 'instrument' in instrument:
    #             instrument_new['instrument'] = instrument['instrument']
    #         instrument_new['_id'] = instrument['_id']
    #         instrument_new['instrumentid'] =  instrument['instrumentid']
    #         instruments_new.append(instrument_new)
    #     return instruments_new

    def load_new_map(self):
        self.full_exchange_map = active_exchange_map
        for exch in self.full_exchange_map:
            self.full_exchange_map_by_name[exch['exchange']] = int(exch['exchangeid'])
            self.full_exchange_map_by_id[int(exch['exchangeid'])] = exch['exchange']

    def add_exchange(self, exchangeid: int()):
        '''
            Maps a new exchange if it is not mapped already
        '''
        if self.full_exchange_map_by_id.get(exchangeid, None) is None:
            # The new exchangeid does not exist in the global map -> raise an error
            print("Exchange id " + str(exchangeid) + " is not in the global map")
            raise KeyError
        else:
            # The exchangeid is in the global map -> gets the info and maps it if not already mapped
            if self.exchange_map_by_id.get(exchangeid, None) is None:
                exchange = self.full_exchange_map_by_id[exchangeid]
                self.exchange_map_by_id[exchangeid] = exchange
                self.exchange_map_by_name['exchange'] = exchangeid
                self.exchanges.append({'exchangeid':exchangeid, 'exchange':exchange, 'active':True})

    def add_instrument(self, instrumentid: int(), instrument):
        self.instruments_by_name[instrument] = int(instrumentid)
        self.instruments_by_id[int(instrumentid)] = instrument

    def add_symbol(self, symbolid: int(), symbol):
        self.symbols_by_name[symbol] = int(symbolid)
        self.symbols_by_id[int(symbolid)] = symbol

    def get_exchange_status(self, exchangeid):
        for exchange in active_exchange_map:
            if exchange['exchangeid'] == exchangeid:
                return exchange['active']

    def get_symbols(self):
        return self.symbols

    def set_instruments(self):
        self.instruments = []
        for id, name in self.instruments_by_id.items():
            data = {'instrumentid': id,
                    'instrumentname': name}
            self.instruments.append(data)

    def set_symbols(self):
        self.symbols = []
        for s, symbolname in self.symbols_by_id.items():
            if symbolname is not None and "/" in symbolname:
                base_instrument = str(symbolname.split("/")[0])
                quote_instrument = str(symbolname.split("/")[1])
                data = {'symbolid': int(s),
                        'symbolname': symbolname,
                        'base_instrument': base_instrument,
                        'quote_instrument': quote_instrument,
                        'base_instrumentid': self.get_instrumentid(base_instrument),
                        'quote_instrumentid': self.get_instrumentid(quote_instrument)}
            else:
                data = {'symbolid': int(s),
                        'symbolname': symbolname,
                        'base_instrument': None,
                        'quote_instrument': None,
                        'base_instrumentid': None,
                        'quote_instrumentid': None}
                print('!!!NO INTRUMENTS FOR :', s, symbolname)
            self.symbols.append(data)

    def get_exchanges(self):
        return self.exchanges

    def generate_new_instrumentid(self):
        if self.get_instrumentids():
            return max(self.get_instrumentids()) + 1
        else:
            return 1

    def generate_new_symbolid(self):
        if self.get_symbolids():
            return max(self.get_symbolids()) + 1
        else:
            return 1

    def get_or_create_instrumentid(self, instrument):
        if instrument in self.instruments_by_name:
            return int(self.instruments_by_name[instrument])
        else:
            self.add_instrument(int(self.generate_new_instrumentid()), instrument)
            return int(self.get_or_create_instrumentid(instrument))

    def get_or_create_symbolid(self, symbol):
        if symbol in self.symbols_by_name:
            return int(self.symbols_by_name[symbol])
        else:
            self.add_symbol(int(self.generate_new_symbolid()), symbol)
            return int(self.get_or_create_symbolid(symbol))

    def insert_book(self, book):
        if BookField.BOOKID.value not in book:
            print(f'Book does not have id, passing.')
            self.log_content.append('Book does not have id, passing.')
            return

        if int(book['bookid']) in self.static_books_by_id:
            print(f"Error -> dupe bookid when inserting, bookid={book['bookid']}, passing")
            self.log_content.append(f"Error -> dupe bookid when inserting, bookid={book['bookid']}, passing")
            return

        if 'exchangeid' not in book:
            print(f"Error -> no exchangeid for book {book['bookid']}")
            self.log_content.append(f"Error -> no exchangeid for book {book['bookid']}")
            return

        if int(book['exchangeid']) not in self.exchange_map_by_id.keys():
            if 'exchange' not in book:
                print(f"Error -> no bookname for book {book['bookid']}")
                self.log_content.append(f"Error -> no bookname for book {book['bookid']}")
                exchange_name = f"Exchange_{book['exchangeid']}"
            else:
                exchange_name = book['exchange']

            self.add_exchange(int(book['exchangeid']))

        self.static_books_by_id[int(book['bookid'])] = book

    def get_static_books(self):
        return self.books

    def get_instruments(self):
        return self.instruments

    def get_static_books_by_id(self):
        return self.static_books_by_id

    def get_static_bookids(self):
        return self.static_books_by_id.keys()

    def filter_static_per_exchange(self, exchange):
        return [b for b in self.get_books() if b['exchange'] == exchange]

    def get_exchange_id(self, exchange):
        return self.full_exchange_map_by_name[exchange]

    def get_instrumentid(self, instrument):
        return self.instruments_by_name.get(instrument, None)

    def get_full_exchangeids(self):
        return self.full_exchange_map_by_id.keys()

    def get_full_exchange_names(self):
        return self.full_exchange_map_by_id.values()

    def get_exchangeids(self):
        return self.exchange_map_by_id.keys()

    def get_exchange_names(self):
        return self.exchange_map_by_id.values()

    def get_instrumentids(self):
        return self.instruments_by_id.keys()

    def get_symbolids(self):
        return self.symbols_by_id.keys()

    def get_bookids(self):
        return self.static_books_by_id.keys()

    def get_books(self):
        return self.static_books_by_id.values()

    def get_static_books(self):
        return self.books

    def get_static_books_by_id(self):
        return self.static_books_by_id

    def get_instruments_names(self):
        return self.instruments_by_id.values()

    def get_symbols_names(self):
        return self.symbols_by_id.values()

    def get_symbolid(self, symbol):
        return self.symbols_by_name[symbol]

    def get_exchange_name(self, exchangeid):
        return self.exchange_map_by_id[exchangeid]

    def get_instrument_name(self, instrumentid):
        return self.instruments_by_id[instrumentid]

    def get_symbol_name(self, symbolid):
        return self.symbols_by_name[symbolid]

    def get_book_by_id(self, bookid):
        return self.static_books_by_id[bookid]

    def get_drivers(self):
        return self.drivers

    def add_driver(self, driver, exchanges_to_update):
        if driver.exchangeid in exchanges_to_update:
            self.drivers.append(driver)

    def get_ccxt_exchangename(self, exchangeid):
        return self.ccxt_exchangemap[exchangeid]

    def get_our_exchangeid_from_ccxtname(self, ccxtname):
        return self.reverse_ccxt_exchangemap[ccxtname]

    def set_ccxt_exchangemap(self, exchangemap):
        self.ccxt_exchangemap = exchangemap
        self.reverse_ccxt_exchangemap = {}
        for k in exchangemap.keys():
            self.reverse_ccxt_exchangemap[exchangemap[k]] = k

    def create_drivers_for_all_exchanges(self):
        for e in self.get_full_exchangeids():
            exchange_name = self.get_exchange_name(e)
            ccxt_exchangename = self.get_ccxt_exchangename(e)
            print(f'Creating driver for exchangeid={e}, exchangename={exchange_name}, ccxtname={ccxt_exchangename}')
            self.log_content.append(
                f'Creating driver for exchangeid={e}, exchangename={exchange_name}, ccxtname={ccxt_exchangename}')
            self.add_driver(CcxtPublic(ccxt_exchangename))

    def get_books_from_exchange(self, exchangeid):
        books = []
        if exchangeid not in self.get_full_exchangeids():
            print(f"/!\ /!\ /!\ Exchange id {str(exchangeid)} is not in the global map!")
            # raise KeyError()
            return books
        books = [b for b in self.get_books() if b.get(BookField.EXCHANGEID.value, -1) == exchangeid]
        return books

    def get_book(self, bookname, exchangeid):
        book = [b for b in self.get_books_from_exchange(exchangeid) if b[BookField.BOOKNAME.value] == bookname]
        return book[0] if book else []

    def get_bookname_from_bookid(self, bookid):
        for b in self.get_books():
            if b[BookField.BOOKID.value] == bookid:
                return b[BookField.BOOKNAME.value]

    def get_exchangeid_from_bookid(self, bookid):
        for b in self.get_books():
            if b[BookField.BOOKID.value] == bookid:
                return b[BookField.EXCHANGEID.value]

    def book_exists(self, bookname, exchangeid):
        return self.get_book(bookname, exchangeid) != []

    def get_bookid(self, bookname, exchangeid):

        def _generate_new_bookid():
            if self.get_bookids():
                return int(max(self.get_bookids()) + 1)
            else:
                return 1

        if self.book_exists(bookname, exchangeid):
            return int(self.get_book(bookname, exchangeid)[BookField.BOOKID.value])
        else:
            return _generate_new_bookid()

    def get_typeid(self, type:str())->int():
        if type in book_types_map.keys():
            return book_types_map[type]
        else:
            print('!!! NO TYPE: ', type)
            return None

    def get_book_type_from_typeid(self, typeid: int())-> str():
        for k, v in book_types_map.items():
            if v == typeid:
              return k
        else:
            print('!!! NO TYPEID: ', typeid)
            return None

    def internalids_mapping(self, proposed_book, exchangeid):

        bookid = self.get_bookid(proposed_book[BookField.BOOKNAME.value], exchangeid)
        proposed_book[BookField.BOOKID.value] = bookid

        if proposed_book[BookField.BOOK_TYPE.value] == BookTypes.LOAN.value:
            proposed_book[BookField.BOOK_TYPE_ID.value] = self.get_typeid(proposed_book[BookField.BOOK_TYPE.value])
            proposed_book[BookField.LOAN_INSTRUMENT_ID.value] = self.get_or_create_instrumentid(proposed_book[BookField.LOAN_INSTRUMENT.value])
            return proposed_book
        else:
            proposed_book[BookField.SYMBOL.value] = str(proposed_book[BookField.BASE.value]) + '/' + str(proposed_book[BookField.QUOTE.value])
            proposed_book[BookField.BASE_INSTRUMENTID.value] = self.get_or_create_instrumentid(proposed_book[BookField.BASE.value])
            proposed_book[BookField.QUOTE_INSTRUMENTID.value] = self.get_or_create_instrumentid(proposed_book[BookField.QUOTE.value])
            proposed_book[BookField.SYMBOLID.value] = self.get_or_create_symbolid(proposed_book[BookField.SYMBOL.value])

            if proposed_book.get(BookField.UNIT_INSTRUMENT.value, None):
                proposed_book[BookField.UNIT_INSTRUMENTID.value] = self.get_or_create_instrumentid(proposed_book[BookField.UNIT_INSTRUMENT.value])

            if proposed_book.get(BookField.LAYER_FEE_INSTRUMENT, None):
                proposed_book[BookField.LAYER_FEE_INSTRUMENTID.value] = self.get_or_create_instrumentid(proposed_book[BookField.LAYER_FEE_INSTRUMENT.value])

            if proposed_book.get(BookField.PROTOCOL_FEE_INSTRUMENT, None):
                proposed_book[BookField.PROTOCOL_FEE_INSTRUMENTID.value] = self.get_or_create_instrumentid(proposed_book[BookField.PROTOCOL_FEE_INSTRUMENT.value])

            if proposed_book.get(BookField.FEE_INSTRUMENT.value, None):
                proposed_book[BookField.FEE_INSTRUMENTID.value] = self.get_or_create_instrumentid(proposed_book[BookField.FEE_INSTRUMENT.value])

            if BookField.BOOK_TYPE.value in proposed_book:
                proposed_book[BookField.BOOK_TYPE_ID.value] = self.get_typeid(proposed_book[BookField.BOOK_TYPE.value])
                proposed_book[BookField.EXCHANGE.value] = self.get_exchange_name(exchangeid)
                proposed_book = self.cleaning_existing_book_ids(proposed_book, exchangeid)
                proposed_book = self.delete_deprecated_fields(proposed_book)
                return proposed_book
            else:
                print('!!!NO BOOKTYPE!!!', proposed_book)

    def round_scientific(self, scientific_value):
        if 'e' in str(scientific_value):
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

    def start_cleaning_per_exchange(self, exchangeid : int()):
        for book in self.get_books_from_exchange(exchangeid):
            book[BookField.ACTIVE.value] = False
            cleaned_book = book
            cleaned_book = self.clean_scientific(cleaned_book)
            cleaned_book = self.match_underlyings_with_bookid(cleaned_book, exchangeid)
            cleaned_book = self.delete_deprecated_fields(cleaned_book)
            self.log_differences(book, cleaned_book, exchangeid)
            self.accept_differences(book, cleaned_book)
            """
            if exchangeid == 20:
                if BookField.BOOK_TYPE.value not in book or BookField.EXCHANGE.value not in book:
                    self.delete_book_from_static(book[BookField.BOOKID.value])
                elif book[BookField.BOOK_TYPE.value] != 'spot' and book[BookField.EXCHANGE.value] == 'OKEX_CSF':
                    self.delete_book_from_static(book[BookField.BOOKID.value])
                else:
                    book[BookField.EXCHANGE.value] = 'OKEX_C'
            """

    def create_new_book_from_proposed_book(self, proposed_book, exchangeid):
        proposed_book[BookField.BOOKID.value] = self.get_bookid(proposed_book[BookField.BOOKNAME.value], exchangeid)
        proposed_book[BookField.EXCHANGEID.value] = exchangeid
        proposed_book[BookField.EXCHANGE.value] = self.get_exchange_name(exchangeid)
        proposed_book = self.modify_existing_book_from_proposed_book(proposed_book, exchangeid)
        return proposed_book

    def modify_existing_book_from_proposed_book(self, proposed_book, exchangeid):
        proposed_book = self.internalids_mapping(proposed_book, exchangeid)
        proposed_book = self.match_underlyings_with_bookid(proposed_book, exchangeid)
        proposed_book = self.clean_formating(proposed_book)
        return proposed_book

    def cleaning_existing_book_ids(self, current_book, exchangeid):
        if BookField.BASE.value in current_book:
            current_book[BookField.BASE_INSTRUMENTID.value] = self.get_or_create_instrumentid(current_book[BookField.BASE.value])
            if current_book[BookField.BASE.value] == 'XBT':
                current_book[BookField.BASE.value] = 'BTC'
        if BookField.QUOTE.value in current_book:
            current_book[BookField.QUOTE_INSTRUMENTID.value] = self.get_or_create_instrumentid(current_book[BookField.QUOTE.value])
            if current_book[BookField.QUOTE.value] == 'XBT':
                current_book[BookField.QUOTE.value] = 'BTC'
        if BookField.BASE.value in current_book and BookField.QUOTE.value in current_book:
            current_book[BookField.SYMBOL.value] = str(current_book[BookField.BASE.value]) + '/' + str(current_book[BookField.QUOTE.value])
        if BookField.SYMBOL.value in current_book:
            current_book[BookField.SYMBOLID.value] = self.get_or_create_symbolid(current_book[BookField.SYMBOL.value])
        if BookField.SETTLEMENT_INSTRUMENT.value in current_book:
            current_book[BookField.SETTLEMENT_INSTRUMENTID.value] = self.get_or_create_instrumentid(current_book[BookField.SETTLEMENT_INSTRUMENT.value])
        if BookField.UNIT_INSTRUMENT.value in current_book:
            current_book[BookField.UNIT_INSTRUMENTID.value] = self.get_or_create_instrumentid(current_book[BookField.UNIT_INSTRUMENT.value])
        if BookField.MARGIN_INSTRUMENT.value in current_book:
            current_book[BookField.MARGIN_INSTRUMENTID.value] = self.get_or_create_instrumentid(current_book[BookField.MARGIN_INSTRUMENT.value])
        if BookField.FEE_INSTRUMENT.value in current_book:
            current_book[BookField.FEE_INSTRUMENTID.value] = self.get_or_create_instrumentid(
                current_book[BookField.FEE_INSTRUMENT.value])
        return current_book

    def delete_deprecated_fields(self, current_book: dict())->dict():
        if 'type' in current_book:
            current_book[BookField.BOOK_TYPE.value] = current_book['type']
            del current_book['type']
            del current_book['typeid']
        if 'quote_instrument_id' in current_book:
            del current_book['quote_instrument_id']
        if 'base_instrument_id' in current_book:
            del current_book['base_instrument_id']
        if 'symbol_id' in current_book:
            del current_book['symbol_id']
        if 'remote_bookname' in current_book:
            del current_book['remote_bookname']
        if 'lower_bookname' in current_book:
            del current_book['lower_bookname']
        if 'isDarkPool' in current_book:
            current_book[BookField.IS_DARK_POOL] = current_book['isDarkPool']
            del current_book['isDarkPool']
        if 'settlement_ccy' in current_book:
            del current_book['settlement_ccy']
        if 'margin_ccy' in current_book:
            del current_book['margin_ccy']
        if 'ws_api_multiplier' in current_book:
            del current_book['ws_api_multiplier']
        if 'fix_api_multiplier' in current_book:
            del current_book['fix_api_multiplier']
        if 'rest_api_multiplier' in current_book:
            del current_book['rest_api_multiplier']
        if 'features' in current_book:
            del current_book['features']
        if BookField.BOOK_TYPE_ID.value in current_book:
            if BookField.FUNDING_PILLARS.value in current_book.keys() and current_book[BookField.BOOK_TYPE_ID.value] not in [101, 102, 103]:
                print('pillars deleted for:', current_book[BookField.BOOKNAME.value], current_book[BookField.EXCHANGE.value])
                del current_book[BookField.FUNDING_PILLARS.value]
        return current_book

    def clean_formating(self, current_book):
        if BookField.PRICE_PRECISION.value in current_book and current_book[BookField.PRICE_PRECISION.value] is not None:
            current_book[BookField.PRICE_PRECISION.value] = int(current_book[BookField.PRICE_PRECISION.value])
        if BookField.QTY_PRECISION.value in current_book and current_book[BookField.QTY_PRECISION.value] is not None:
            current_book[BookField.QTY_PRECISION.value] = int(current_book[BookField.QTY_PRECISION.value])
        if BookField.TICK_SIZE.value in current_book and current_book[BookField.TICK_SIZE.value] is not None:
            current_book[BookField.TICK_SIZE.value] = float(current_book[BookField.TICK_SIZE.value])
        if BookField.CONTRACT_SIZE.value in current_book and current_book[BookField.CONTRACT_SIZE.value] is not None:
            current_book[BookField.CONTRACT_SIZE.value] = int(current_book[BookField.CONTRACT_SIZE.value])
        if BookField.UNIT_SIZE.value in current_book and current_book[BookField.UNIT_SIZE.value] is not None:
            current_book[BookField.UNIT_SIZE.value] = float(current_book[BookField.UNIT_SIZE.value])
        if BookField.QTY_INCREMENT.value in current_book and current_book[BookField.QTY_INCREMENT.value] is not None:
            current_book[BookField.QTY_INCREMENT.value] = float(current_book[BookField.QTY_INCREMENT.value])
        return self.delete_deprecated_fields(current_book)

    def clean_nonactive_books(self, modified_book):
        if BookField.ACTIVE.value not in modified_book:
            modified_book[BookField.ACTIVE.value] = False
        return modified_book

    def insert_proposed_book(self, proposed_book, exchangeid):
        new_book = self.create_new_book_from_proposed_book(proposed_book, exchangeid)
        self.insert_book(new_book)

    def log_difference(self, bookid, bookname, field, current, suggested, exchangeid):
        print(f"DifferenceAlert for bookid={bookid} bookname={bookname} exchange={exchangeid}: {field} {current} -> {suggested}")
        self.log_content.append(
            f"DifferenceAlert for bbookid={bookid} bookname={bookname} exchange={exchangeid}: {field} {current} -> {suggested}")

    def log_insertion(self, proposed_book):
        print(f'NewBookInsertion: {proposed_book}')
        self.log_content.append(f'NewBookInsertion: {proposed_book}')

    def log_differences(self, current_book, proposed_book, exchangeid):
        for field in proposed_book.keys():
            if field is not BookField.ACTIVE.value:
                if field not in current_book:
                    current_value = None
                else:
                    current_value = current_book[field]
                if field not in proposed_book:
                    proposed_value = None
                else:
                    proposed_value = proposed_book[field]
                if current_value != proposed_value:
                    self.log_difference(current_book[BookField.BOOKID.value], current_book[BookField.BOOKNAME.value], field, current_value, proposed_value,
                                        exchangeid)
            else:
                if current_book[field] is not None and current_book[field] is True:
                    if proposed_book[field] is False:
                        self.log_difference(current_book[BookField.BOOKID.value], current_book[BookField.BOOKNAME.value],
                                        field, current_book[field], proposed_book[field],
                                        exchangeid)

    def modify_book(self, bookid, field, value):
        self.static_books_by_id[bookid][field] = value

    def accept_differences(self, current_book, proposed_book):
        for field in proposed_book.keys():
            if field not in current_book.keys():
                current_value = None
            else:
                current_value = current_book[field]
            proposed_value = proposed_book[field]
            if current_value != proposed_value:
                self.modify_book(current_book[BookField.BOOKID.value], field, proposed_value)

    def construct_static_data_for_export(self):

        self.set_instruments()
        self.set_symbols()

        # test this one please
        static_data = {}
        static_data['books'] = [b for b in self.get_books()]
        static_data['exchanges'] = self.get_exchanges()
        static_data['instruments'] = self.get_instruments()
        static_data['symbols'] = self.get_symbols()
        return static_data

    def construct_static_data_for_checks_and_dashboard(self) -> dict():

        self.set_symbols()

        # test this one please
        static_data = {}
        static_data['books'] = {}
        static_data['exchanges'] = {}
        static_data['instruments'] = {}
        static_data['symbols'] = {}

        for k, v in self.get_static_books_by_id().items():
            static_data['books'][int(k)] = v
        for k, v in self.exchange_map_by_id.items():
            static_data['exchanges'][int(k)] = v
        for k, v in self.instruments_by_id.items():
            static_data['instruments'][int(k)] = v
        for symbol in self.symbols:
            static_data['symbols'][int(symbol['symbolid'])] = symbol
        return static_data

    def load_static_from_file_by_id(self, static_file: dict()) -> dict():
        static_data = dict()
        static_data['books'] = dict()
        for book in static_file['books']:
            static_data['books'][int(book['bookid'])] = book
        static_data['symbols'] = dict()
        for symbol in static_file['symbols']:
            static_data['symbols'][int(symbol['symbolid'])] = symbol
            static_data['symbols'][int(symbol['symbolid'])]['_id'] = symbol['symbolid']
        static_data['instruments'] = dict()
        for instrument in static_file['instruments']:
            static_data['instruments'][int(instrument['instrumentid'])] = instrument
            static_data['instruments'][int(instrument['instrumentid'])]['_id'] = instrument['instrumentid']
        static_data['exchanges'] = dict()
        for exchange in static_file['exchanges']:
            static_data['exchanges'][int(exchange['exchangeid'])] = exchange
            static_data['exchanges'][int(exchange['exchangeid'])]['_id'] = exchange['exchangeid']
        return static_data

    def construct_log_for_export(self):
        text = 'The log of the static file is :'
        for line in self.log_content:
            text = text + str(line) + '\n'
        return text

    def dump_file_static_data(self):
        Path(self.staticdata_path).mkdir(parents=True, exist_ok=True)
        with open(self.file_name, 'w', encoding='utf-8') as f:
            json.dump(self.construct_static_data_for_export(), f, ensure_ascii=False, indent=4)

    def dump_static_for_checks_and_dashboard(self):
        Path(self.staticchecker_path).mkdir(parents=True, exist_ok=True)
        with open(self.check_file_name, 'w', encoding='utf-8') as f:
            json.dump(self.construct_static_data_for_checks_and_dashboard(), f, ensure_ascii=False, indent=4)

    def dump_log_static_data(self):
        Path(self.staticdata_logs_path).mkdir(parents=True, exist_ok=True)
        with open(self.log_name, 'w', encoding='utf-8') as f:
            for line in self.log_content:
                f.write(str(line) + '\n')

    def dump_log_for_checks_and_dashboard(self):
        Path(self.staticdatachecker_logs_path).mkdir(parents=True, exist_ok=True)
        with open(self.check_log_name, 'w', encoding='utf-8') as f:
            for line in self.log_content:
                f.write(str(line) + '\n')

    def clean_kraken_book_type(self, book):
        if book[BookField.EXCHANGEID.value] == 6:
            book[BookField.BOOK_TYPE.value] = 'spot'
            book[BookField.BOOK_TYPE_ID.value] = 0
            if '.d' in book[BookField.BOOKNAME]:
                book[BookField.CCXT_ID] = book[BookField.BOOKNAME.value]
                book[BookField.IS_DARK_POOL.value] = True
        return book

    def add_ccxt_id_in_old_kraken_books(self, book):
        if book[BookField.EXCHANGEID.value] == 6:
            if BookField.CCXT_ID.value not in book:
                book[BookField.CCXT_ID.value] = book[BookField.BOOKNAME.value]
        return book

    def clean_static_data(self):

        def clean_missing_booktype(self, book: dict()) -> dict():
            if BookField.BOOK_TYPE_ID.value in book.keys():
                book[BookField.BOOK_TYPE.value] = self.get_book_type_from_typeid(book[BookField.BOOK_TYPE_ID.value])
            return book

        def clean_old_books_underlying_mapping(self, old_book):
            book = old_book
            if 'index_name' in book and BookField.UNDERLYING_BOOKNAME.value in book:
                del book['index_name']
            if 'index_name' in book and BookField.UNDERLYING_BOOKNAME.value not in book:
                book[BookField.UNDERLYING_BOOKNAME.value] = book['index_name']
                del book['index_name']
            if BookField.UNDERLYING_BOOKNAME.value in book:
                book[BookField.UNDERLYING_BOOKID.value] = self.get_bookid(book[BookField.UNDERLYING_BOOKNAME.value],
                                                                          book[BookField.EXCHANGEID.value])
            if BookField.UNDERLYING_BOOKID.value in old_book:
                if old_book[BookField.UNDERLYING_BOOKID.value] != book[BookField.UNDERLYING_BOOKID.value]:
                    print('!!!!!!! bookid changed!!!!', book[BookField.BOOKID.value], book[BookField.EXCHANGEID.value],
                          book[BookField.UNDERLYING_BOOKID.value])
            self.log_differences(old_book, book, book[BookField.EXCHANGEID.value])
            return book

        for book in self.get_books():
            cleaned_book = self.cleaning_existing_book_ids(book, book[BookField.EXCHANGEID.value])
            self.log_differences(book, cleaned_book, cleaned_book[BookField.EXCHANGEID.value])
            self.accept_differences(book, cleaned_book)
            cleaned_book = self.clean_formating(book)
            self.log_differences(book, cleaned_book, cleaned_book[BookField.EXCHANGEID.value])
            self.accept_differences(book, cleaned_book)
            cleaned_book = clean_missing_booktype(book)
            self.log_differences(book, cleaned_book, cleaned_book[BookField.EXCHANGEID.value])
            self.accept_differences(book, cleaned_book)
            cleaned_book = clean_old_books_underlying_mapping(book)
            self.log_differences(book, cleaned_book, cleaned_book[BookField.EXCHANGEID.value])
            self.accept_differences(book, cleaned_book)
            cleaned_book = self.delete_deprecated_fields(book)
            self.log_differences(book, cleaned_book,  cleaned_book[BookField.EXCHANGEID.value])
            self.accept_differences(book, cleaned_book)
            cleaned_book[BookField.ACTIVE.value] = False
            self.log_differences(book, cleaned_book, cleaned_book[BookField.EXCHANGEID.value])
            self.accept_differences(book, cleaned_book)


    def clean_bookname_duplicates(self, exchangeid):

        def get_mult_bookid(self, bookname, exchangeid):
            booklist = []
            for b in self.get_books():
                if b[BookField.BOOKNAME.value] == bookname and b[BookField.EXCHANGEID.value] == exchangeid:
                    booklist.append(b[BookField.BOOKID.value])
            return booklist

        book_names = []

        for b in list(self.get_books()):
            if b[BookField.EXCHANGEID.value] == exchangeid:
                book_names.append(b[BookField.BOOKNAME.value])
                duplicates = [item for item, count in collections.Counter(book_names).items() if count > 1]
                for bookname in duplicates:
                    for bookid in get_mult_bookid(bookname, exchangeid)[1:]:
                        self.delete_book_from_static(bookid)

    def delete_book_from_static(self, bookid):
        print(
            f"{bookid} was deleted : {self.get_bookname_from_bookid(bookid)} {self.get_exchangeid_from_bookid(bookid)}")
        self.log_content.append(
            f"{bookid} was deleted : {self.get_bookname_from_bookid(bookid)} {self.get_exchangeid_from_bookid(bookid)}")
        del self.static_books_by_id[bookid]


    def clean_ftx_contract_size(self, book):
        if book[BookField.EXCHANGEID.value] == 23:
            if BookField.CONTRACT_SIZE.value in book:
                print('FTX contract size deleted ', book[BookField.BOOKNAME.value], book[BookField.BOOK_TYPE.value])
                del book[BookField.CONTRACT_SIZE.value]
        return book

    def clean_kraken_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book['exchangeid'] == 6:
                if '.d' not in book['bookname']:
                    if len(book['bookname']) >= 8:
                        if book['bookname'][0] == 'X' and book['bookname'][4] == 'X':
                            self.log_content.append(f"deleted: {book}")
                            print(f"deleted : {book}")
                            self.delete_book_from_static(bookid)
                        if book['bookname'][0] == 'Z' and book['bookname'][4] == 'Z':
                            self.log_content.append(f"deleted: {book}")
                            print(f"deleted : {book}")
                            self.delete_book_from_static(bookid)
                        if book['bookname'][0] == 'Z' and book['bookname'][4] == 'X':
                            self.log_content.append(f"deleted: {book}")
                            print(f"deleted : {book}")
                            self.delete_book_from_static(bookid)
                        if book['bookname'][0] == 'X' and book['bookname'][4] == 'Z':
                            self.log_content.append(f"deleted: {book}")
                            print(f"deleted : {book}")
                            self.delete_book_from_static(bookid)
                    if book[BookField.ACTIVE.value] is False and '/' not in book[BookField.BOOKNAME.value]:
                        self.log_content.append(f"deleted: {book}")
                        print(f"deleted : {book}")
                        self.delete_book_from_static(bookid)

    def clean_poloniex_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book['exchangeid'] == 5:
                if '-' in book['bookname']:
                    print(f"deleted : {book}")
                    self.log_content.append(f"deleted: {book}")
                    self.delete_book_from_static(bookid)

    def clean_binance_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book['exchangeid'] == 4:
                if '-' in book['bookname']:
                    print(f"Deleted : {book}")
                    self.log_content.append(f"Deleted :{book}")
                    self.delete_book_from_static(bookid)

    def clean_exx_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book['exchangeid'] == 16:
                if '-' in book['bookname']:
                    print(f"deleted : {book}")
                    self.log_content.append(f"deleted : {book}")
                    self.delete_book_from_static(bookid)

    def clean_binanceDM_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book['exchangeid'] == 15:
                if '-' in book['bookname']:
                    print(f"deleted : {book}")
                    self.log_content.append(f"deleted : {book}")
                    self.delete_book_from_static(bookid)

    def clean_bitfinex_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book['exchangeid'] == 2:
                if 't' not in book['bookname']:
                    print(f"deleted : {book}")
                    self.log_content.append(f"deleted : {book}")
                    self.delete_book_from_static(bookid)

    def clean_huobi_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book['exchangeid'] == 17:
                if '-' in book['bookname']:
                    print(f"deleted : {book}")
                    self.log_content.append(f"deleted : {book}")
                    self.delete_book_from_static(bookid)

    def clean_hitbtc_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book['exchangeid'] == 11:
                if '-' in book['bookname']:
                    print(f"deleted : {book}")
                    self.log_content.append(f"deleted : {book}")
                    self.delete_book_from_static(bookid)

    def clean_bittrex_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book[BookField.EXCHANGEID.value] == 8:
                if '-' in book[BookField.BOOKNAME.value] and book[BookField.ACTIVE.value] is False:
                    if book[BookField.BASE.value] != str(book[BookField.BOOKNAME.value]).split('-')[0]:
                        print(f"deleted : {book}")
                        self.log_content.append(f"deleted : {book}")
                        self.delete_book_from_static(bookid)

    def clean_ascendex_dups(self):
        for bookid, book in list(self.get_static_books_by_id().items()):
            if book['exchangeid'] == 25:
                if '-' in book['bookname'] and 'PERP' not in book['bookname']:
                    print(f"deleted : {book}")
                    self.log_content.append(f"deleted : {book}")
                    self.delete_book_from_static(bookid)

    def match_underlyings_with_bookid(self, book, exchangeid):
        bookids = []
        if BookField.INDEX.value in book and book[BookField.INDEX.value] is not None and len(book[BookField.INDEX.value]) > 0:
            for pair in book[BookField.INDEX.value][BookField.BOOKS.value]:
                bookids.append(self.get_bitmex_bookids(pair[BookField.BOOKNAME.value], pair[BookField.EXCHANGEID.value]))
            book[BookField.INDEX.value]['bookids'] = bookids
        if BookField.UNDERLYING_BOOKNAME.value in book and book[BookField.UNDERLYING_BOOKNAME.value] is not None:
            if BookField.UNDERLYING_EXCHANGEID.value in book:
                book[BookField.UNDERLYING_BOOKID.value] = self.get_bookid(book[BookField.UNDERLYING_BOOKNAME.value],
                                                                      book[BookField.UNDERLYING_EXCHANGEID.value])
            else:
                book[BookField.UNDERLYING_BOOKID.value] = self.get_bookid(book[BookField.UNDERLYING_BOOKNAME.value],
                                                                          exchangeid)
            if book[BookField.UNDERLYING_BOOKID.value] is None:
                book[BookField.UNDERLYING_BOOKID.value] = int(0)
            if book.get(BookField.PASSIVE_FEE_BPS.value, None) is None:
                book[BookField.PASSIVE_FEE_BPS.value] = float(0)
            if book.get(BookField.ACTIVE_FEE_BPS.value, None) is None:
                book[BookField.ACTIVE_FEE_BPS.value] = float(0.1)
        return book

    def get_bitmex_bookids(self, bookname, exchangeid):
        for b in self.get_books_from_exchange(exchangeid):
            if exchangeid == 6:
                if b[BookField.CCXT_ID.value] == bookname:
                    return b[BookField.BOOKID.value]
            else:
                if b[BookField.BOOKNAME.value].lower() == bookname.lower():
                    return b[BookField.BOOKID.value]

    def get_path_latest_static_file(self):
        try:
            folder_path = self.staticdata_path
            file_type = '/*json'
            return str(max(glob.iglob(folder_path + file_type), key=os.path.getctime))
        except ValueError:
            pass
        except TypeError:
            pass

    def build_static(self):
        '''
        Ongoing -> Load last static and log differences
        :return:
        '''

        # self.load_all_statics(um.load_static_from_db())

        # Try to load the latest static if it exists
        try:
            self.load_static_from_file(self.get_path_latest_static_file())
        except ValueError:
            # Static does not exist, ignoring old static
            pass
        except TypeError:
            pass

        # self.clean_static_data()
        # self.clean_binanceDM_dups()
        # self.clean_bittrex_dups()
        # self.clean_poloniex_dups()
        # self.clean_hitbtc_dups()
        # self.clean_huobi_dups()
        # self.clean_exx_dups()
        # self.clean_binance_dups()
        # self.clean_kraken_dups()
        # self.clean_bitfinex_dups()

        print(f"\nnb of exchanges to parse : {len(self.get_drivers())} \n")
        print('old exchanges total nb :', len(self.get_exchanges()))
        print('old instruments total nb :', len(self.get_instruments()))
        print('old symbols total nb :', len(self.get_symbols()))
        print('old books total nb :', len(self.get_static_books()))

        for d in self.get_drivers():

            exchangeid = d.get_exchangeid()

            # Maps the new exchange if not already mapped in the static
            self.add_exchange(exchangeid)

            self.start_cleaning_per_exchange(exchangeid)
            for book in d.get_books():
                if d.get_exchange_type(book) == 'option':
                    continue
                exchange_bookname = d.get_bookname(book)
                proposed_book = d.get_proposed_book(book)
                # Mapping is only intended if proposed_book != {'exchangeid':x} which is the default
                if len(proposed_book.keys()) > 1 :
                    if self.book_exists(exchange_bookname, exchangeid):
                        current_book = self.get_book(exchange_bookname, exchangeid)
                        proposed_book = self.modify_existing_book_from_proposed_book(proposed_book, exchangeid)
                        self.log_differences(current_book, proposed_book, exchangeid)
                        self.accept_differences(current_book, proposed_book)
                    else:
                        self.insert_proposed_book(proposed_book, exchangeid)
                        self.log_insertion(proposed_book)

        # self.clean_bookname_duplicates(exchangeid)
        self.get_file_name()
        self.get_log_name()

        # Dump the static
        self.dump_log_static_data()
        self.dump_static_for_checks_and_dashboard()
        self.dump_log_for_checks_and_dashboard()
        self.dump_file_static_data()

        print('\nnew exchanges total nb :', len(self.get_exchanges()))
        print('new instruments total nb :', len(self.get_instrumentids()))
        print('new symbols total nb :', len(self.get_symbolids()))
        print('new books total nb :', len(self.get_bookids()))
        print(f"\nnb of exchanges parsed : {len(self.get_drivers())} \n")

    def get_new_static(self):
        return self.construct_static_data_for_export()
