from pylibs.utils.db.mongo_connector import MongoConnector, MongoInfo, MongoData
from pylibs.staticdata.checker.log_changes import LogTypes
from pprint import pprint
import json
from pathlib import Path
import glob, time
import os.path
from pathlib import Path
from datetime import timedelta, datetime
from bson.objectid import ObjectId
mongo_info = "" # MongoInfo(MongoData.STATIC_DATA_DB, MongoData.STATIC_PERSISTOR_URI)

class UpdateStaticInDB:
    def __init__(self):
        self.db_changes = "" # MongoConnector(mongo_info, MongoData.CHANGES_COL)
        self.db_books = "" # MongoConnector(mongo_info, MongoData.BOOKS_COL)
        self.db_exchanges = "" # MongoConnector(mongo_info, MongoData.EXCHANGES_COL)
        self.db_instruments = "" # MongoConnector(mongo_info, MongoData.INSTRUMENTS_COL)
        self.db_symbols = "" # MongoConnector(mongo_info, MongoData.SYMBOLS_COL)
        self.home = str(Path.home())
        self.unprocessed_changes = dict()
    def load_static_from_db(self):
        static_data = dict()
        static_data['books'] = [b for b in self.db_books.find_full_collection()]
        static_data['symbols'] = [s for s in self.db_symbols.find_full_collection()]
        static_data['instruments'] = [i for i in self.db_instruments.find_full_collection()]
        static_data['exchanges'] = [e for e in self.db_exchanges.find_full_collection()]
        return static_data
    def load_static_from_db_with_filers(self, filters:dict())->dict():
        static_data = dict()
        static_data['books'] = [b for b in self.db_books.find_query(filters)]
        static_data['symbols'] = [s for s in self.db_symbols.find_full_collection()]
        static_data['instruments'] = [i for i in self.db_instruments.find_full_collection()]
        static_data['exchanges'] = [e for e in self.db_exchanges.find_full_collection()]
        return static_data
    def load_latest_json(self, new_static_filepath):
        with open(new_static_filepath, mode='r', encoding='utf-8') as json_new:
            return json.load(json_new)

    def dump_staticdata(self, static_data: dict(), filename):
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(static_data, f, ensure_ascii=False, indent=4)

    def dump_staticdata_from_db(self, static_data: dict()):
        with open(self.home + '/Static/StaticDump/active_static_' + time.strftime("%Y%m%d-%H%M%S") + '.json', 'w', encoding='utf-8') as f:
            json.dump(static_data, f, ensure_ascii=False, indent=4)

    def construct_static_data_for_push(self, latest_json_by_id: dict()):
        static_to_push = dict()
        static_to_push['books'] = [b for b in latest_json_by_id['books'].values()]
        instruments = []
        for i, v in latest_json_by_id['instruments'].items():
            instruments.append({'instrumentid': i, 'instrumentname': v})
        static_to_push['instruments'] = instruments
        exchange_map = []
        for e, v in latest_json_by_id['exchanges'].items():
            exchange_map.append({'exchangeid': e, 'exchange': v})
        static_to_push['exchanges'] = exchange_map
        static_to_push['symbols'] = [b for b in latest_json_by_id['symbols'].values()]
        return static_to_push

    def get_new_symbols(self, latest_json_by_id:dict()):
        symbols = []
        for s, symbolname in latest_json_by_id['symbols'].items():
            if symbolname is not None and "/" in symbolname:
                base_instrument = str(symbolname.split("/")[0])
                quote_instrument = str(symbolname.split("/")[1])
                symbols.append({'symbolid': int(s), 'symbolname': symbolname,
                                'base_instrument': base_instrument, 'quote_instrument': quote_instrument,
                                'base_instrumentid': self.get_instrumentid(base_instrument),
                                'quote_instrumentid': self.get_instrumentid(quote_instrument)})
            else:
                base_instrument = None
                quote_instrument = None
                symbols.append({'symbolid': int(s), 'symbolname': symbolname,
                                'base_instrument': base_instrument, 'quote_instrument': quote_instrument,
                                'base_instrumentid': None,
                                'quote_instrumentid': None})
                print('!!!NO INTRUMENTS FOR :', s,symbolname)
        return symbols

    def push_latest_static_file_into_db(self, static_to_push, books: bool, instruments: bool, symbols: bool, exchanges: bool):
        if books:
            for book in static_to_push['books']:
                self.db_books.insert_one_into_collection(book, int(book['bookid']))
        if symbols:
            for symbol in static_to_push['symbols']:
                self.db_symbols.insert_one_into_collection(symbol, int(symbol['symbolid']))
        if instruments:
            for instrument in static_to_push['instruments']:
                self.db_instruments.insert_one_into_collection(instrument, int(instrument['instrumentid']))
        if exchanges:
            for exchange in static_to_push['exchanges']:
                self.db_exchanges.insert_one_into_collection(exchange, int(exchange['exchangeid']))
    def remove_current_static_from_db(self, books: bool, instruments: bool, symbols: bool, exchanges: bool):
        if books:
            self.db_books.remove_collection()
        if exchanges:
            self.db_exchanges.remove_collection()
        if symbols:
            self.db_symbols.remove_collection()
        if instruments:
            self.db_instruments.remove_collection()
    def remove_changes_history_from_db(self):
        self.db_changes.remove_collection()
    def get_path_latest_static_by_id(self):
        folder_path = self.home + '/Static/StaticChecker/'
        file_type = '/*json'
        return str(max(glob.iglob(folder_path + file_type), key=os.path.getctime))
    def get_path_latest_static_in_portoshared(self):
        folder_path = self.home + '/portoshared/staticdata/'
        file_type = '/*json'
        return str(max(glob.iglob(folder_path + file_type), key=os.path.getctime))

    def get_latest_static_file_path(self):
        folder_path = self.home + '/Static/'
        file_type = '/*json'
        return str(max(glob.iglob(folder_path + file_type), key=os.path.getctime))

    def pull_all_unprocessed_changes(self):
        self.unprocessed_changes = {}
        changes = self.db_changes.find_query({'approved': False, 'refused': False})
        if len(changes) > 0:
            for change in changes:
                self.unprocessed_changes[str(change['_id'])] = change
        #pprint(self.unprocessed_changes)
    def get_all_unprocessed_changes(self) -> dict():
        return self.unprocessed_changes
    def get_unprocessed_change(self, id):
        return self.unprocessed_changes[id]
    def load_static_from_db_by_id(self)-> dict():
        static_data = dict()
        static_data['books'] = dict()
        for book in self.db_books.find_full_collection():
            static_data['books'][int(book['bookid'])] = book
        static_data['symbols'] = dict()
        for symbol in self.db_symbols.find_full_collection():
            static_data['symbols'][int(symbol['symbolid'])] = symbol
        static_data['instruments'] = dict()
        for instrument in self.db_instruments.find_full_collection():
            static_data['instruments'][int(instrument['instrumentid'])] = instrument
        static_data['exchanges'] = dict()
        for exchange in self.db_exchanges.find_full_collection():
            static_data['exchanges'][int(exchange['exchangeid'])] = exchange
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

    def get_current_static_in_db(self) -> dict():
        return self.current_static

    def push_change_into_static_db(self, change: dict()):
        if change['change_report'] == LogTypes.BOOK_INSERTION:
            self.push_new_book_into_db(change)
        elif change['change_report'] == LogTypes.BOOK_DELETION:
            self.delete_book_from_db(change)
        elif change['change_report'] in [LogTypes.BOOKFIELD_AMENDED,
                                         LogTypes.BOOKFIELD_INSERTED]:
            self.amend_book_in_db(change)
        elif change['change_report'] == LogTypes.BOOKFIELD_DELETED:
            self.delete_bookfield_in_db(change)
        elif change['change_report'] == LogTypes.INSTRUMENTS_INSERTION:
            self.push_new_instrument_into_db(change)
        elif change['change_report'] == LogTypes.SYMBOLS_INSERTION:
            self.push_new_symbol_into_db(change)
        elif change['change_report'] == LogTypes.EXCHANGES_INSERTION:
            self.push_new_exchange_into_db(change)
        elif change['change_report'] == LogTypes.SYMBOLS_AMENDED:
            self.amend_symbol_in_db(change)
        elif change['change_report'] == LogTypes.EXCHANGES_AMENDED:
            self.amend_exchange_in_db(change)
        else:
            print('!!must be refused!! unauthorized change!')

    def auto_refuse_all_unprocessed_changes_in_db(self):
        review_info = dict()
        review_info['refused'] = True
        review_info['review_time'] = datetime.utcnow().isoformat()
        review_info['reviewed_by'] = 'auto'
        #for change in self.db_changes.find_query({'approved': False, 'refused': False}):
           # self.db_changes.update_one_document(review_info, change['_id'])
        self.db_changes.update_many_query({'approved': False, 'refused': False}, review_info)

    def refuse_single_change_in_db(self, _id, user='fpallix'):
        review_info = dict()
        review_info['refused'] = True
        review_info['review_time'] = datetime.utcnow().isoformat()
        review_info['reviewed_by'] = user
        self.db_changes.update_one_document(review_info, _id)

    def approve_single_change_in_db(self, _id, user='fpallix'):
        review_info = dict()
        review_info['approved'] = True
        review_info['review_time'] = datetime.utcnow().isoformat()
        review_info['reviewed_by'] = user
        #pprint(self.get_all_unprocessed_changes())
        self.push_change_into_static_db(self.unprocessed_changes[str(_id)])
        self.db_changes.update_one_document(review_info, _id)

    def approve_all_changes_in_db(self, user='fpallix'):
        print(self.get_all_unprocessed_changes())
        for _id in self.get_all_unprocessed_changes().keys():
            print(_id)
            self.approve_single_change_in_db(_id, user)

    def refuse_all_changes_in_db(self, user='fpallix'):
        print(self.get_all_unprocessed_changes())
        for _id in self.get_all_unprocessed_changes().keys():
            print(_id)
            self.refuse_single_change_in_db(_id, user)

    def push_new_book_into_db(self, change: dict()):
        self.db_books.insert_one_into_collection(change['book'], change['book']['bookid'])

    def delete_book_from_db(self, change: dict()):
        self.db_books.delete_one_doc_with_id(int(change['bookid']))

    def amend_book_in_db(self, change: dict()):
        print({change['bookfield']: change['suggested']}, change['bookid'])
        self.db_books.update_one_document({change['bookfield']: change['suggested']},
                                          int(change['bookid']))

    def amend_symbol_in_db(self, change: dict()):
        print(change['new_value'], change['static_id'])
        self.db_symbols.update_one_document(change['new_value'],
                                          int(change['static_id']))

    def amend_exchange_in_db(self, change: dict()):
        print(change['new_value'], change['old_value']['exchangeid'])
        self.db_exchanges.update_one_document(change['new_value'],
                                            int(change['static_id']))

    def delete_bookfield_in_db(self, change: dict()):
        print({change['bookfield']: change['suggested']}, change['bookid'])
        self.db_books.delete_one_field_with_id({change['bookfield']: change['current']},
                                          int(change['bookid']))

    def push_new_instrument_into_db(self, change: dict()):
        db_change = dict()
        db_change['instrumentid'] = int(change['static_id'])
        db_change['instrumentname'] = change['new_value']['instrumentname']
        self.db_instruments.insert_one_into_collection(db_change, int(change['static_id']))

    def push_new_symbol_into_db(self, change: dict()):
        db_change = dict()
        db_change['symbolid'] = int(change['static_id'])
        db_change['symbolname'] = change['new_value']['symbolname']
        db_change['base_instrument'] = change['new_value']['base_instrument']
        db_change['quote_instrument'] = change['new_value']['quote_instrument']
        db_change['quote_instrumentid'] = change['new_value']['quote_instrumentid']
        db_change['base_instrumentid'] = change['new_value']['base_instrumentid']
        self.db_symbols.insert_one_into_collection(db_change, int(change['static_id']))

    def push_new_exchange_into_db(self, change: dict()):
        db_change = dict()
        db_change['exchangeid'] = int(change['static_id'])
        db_change['exchange'] = change['new_value']['exchange']
        db_change['active'] = change['new_value']['active']
        self.db_exchanges.insert_one_into_collection(db_change, int(change['static_id']))

