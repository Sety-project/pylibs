import json
import datetime
import time
from pathlib import Path
from pylibs.staticdata.generator.book_field import BookField
from pylibs.staticdata.checker.log_types import LogTypes
# from pylibs.utils.db.mongo_connector import MongoConnector, MongoInfo, MongoData
mongo_info = "" # MongoInfo(MongoData.STATIC_DATA_DB, MongoData.STATIC_PERSISTOR_URI)

class LogChanges:

    def __init__(self):
        self.new_book_insertion = {}
        self.book_deletion = {}
        self.fields_amended = {}
        self.duplicated_book = {}
        self.log_content = list()
        self.formatted_log_content = {}
        self.approved_changes = list()
        self.refused_changes = list()
        self.home = str(Path.home())
        self.time_date_stamp = time.strftime("%Y%m%d_%H%M%S")
        self.log_filename = self.home + '/Static/StaticChecker/checker_' + self.time_date_stamp + '.log'
        self.formatted_log_filename = self.home + '/Static/StaticChecker/formatted_checker_' + self.time_date_stamp + '.json'
        #self.db_changes = MongoConnector(mongo_info, MongoData.CHANGES_COL)

    def get_new_book_insertion(self):
        return self.new_book_insertion

    def get_book_deletion(self):
        return self.book_deletion

    def get_fields_amended(self):
        return self.fields_amended

    def get_dupplicated_book(self):
        return self.duplicated_book

    def start_time_logging_diffs(self):
        self.log_content.append({'start_time_logging_diffs': datetime.datetime.utcnow().isoformat()})
        return datetime.datetime.utcnow().isoformat()

    def end_time_logging_diffs(self):
        self.log_content.append({'end_time_logging_diffs': datetime.datetime.utcnow().isoformat()})
        return datetime.datetime.utcnow().isoformat()

    def push_to_db(self, body):
        #ki.push_into_kibana(KibanaIndexNames.STATIC_CHANGE_NEW.value, body)
        # return self.db_changes.insert_one_into_collection(body)
        print(body)

    def append_log(self, log):
        # log['approved'] = False
        # log['refused'] = False
        self.log_content.append(log)

    def get_approved_changes(self) -> list():
        return self.approved_changes

    def get_refused_changes(self) -> list():
        return self.refused_changes

    def get_log_content(self) -> list():
        return self.log_content

    def append_approved_changes(self, change:dict()) -> list():
        return self.approved_changes.append(change)

    def append_refused_changes(self, change:dict()) -> list():
        return self.refused_changes.append(change)

    def dump_log(self, filename):
        with open(filename, 'w', encoding='utf-8') as f:
            for line in self.log_content:
                f.write(str(line) + '\n')

    def dump_formatted_logs(self):
        out_file = open(self.formatted_log_filename, "w")
        json.dump(self.formatted_log_content, out_file, indent=6)
        out_file.close()

    def reset_log_content(self, log_content=list()):
        self.log_content = log_content
        self.approved_changes = list()
        self.refused_changes = list()

    def books_changes_logs(self, log_book, book) -> dict():
        log_book[BookField.BOOKID.value] = int(book[BookField.BOOKID.value])
        log_book[BookField.BOOKNAME.value] = str(book[BookField.BOOKNAME.value])
        if BookField.BOOK_TYPE_ID.value in book:
            log_book[BookField.BOOK_TYPE_ID.value] = int(book[BookField.BOOK_TYPE_ID.value])
        log_book[BookField.EXCHANGEID.value] = int(book[BookField.EXCHANGEID.value])
        if BookField.ACTIVE in book:
            log_book[BookField.ACTIVE.value] = bool(book[BookField.ACTIVE.value])
        log_book['ts'] = datetime.datetime.utcnow().isoformat()
        return log_book

    def fields_changes_logs(self, log_field, bookid, field, current, suggested, book) -> dict():
        log_field[BookField.BOOKID.value] = int(bookid)
        log_field['bookfield'] = field
        log_field['current'] = current
        log_field['suggested'] = suggested
        log_field['exchangeid'] = int(book[BookField.EXCHANGEID.value])
        log_field['bookname'] = str(book[BookField.BOOKNAME.value])
        log_field['active'] = bool(book[BookField.ACTIVE.value])
        log_field['ts'] = datetime.datetime.utcnow().isoformat()
        return log_field

    def add_book_insertion(self, book: dict()) -> dict():
        book_insertion = dict()
        book_insertion['change_report'] = LogTypes.BOOK_INSERTION.value
        book_insertion['book'] = book
        book_insertion['ts'] = datetime.datetime.utcnow().isoformat()
        return book_insertion

    def add_book_deletion(self, book: dict()) -> dict():
        book_deletion = dict()
        book_deletion['change_report'] = LogTypes.BOOK_DELETION.value
        self.books_changes_logs(book_deletion, book)
        return book_deletion

    def add_book_duplicate(self, similar_fields: dict(),books: dict()) -> dict():
        book_duplicate = dict()
        book_duplicate['change_report'] = LogTypes.SUSPECTED_DUPLICATE.value
        book_duplicate['similar_bookfields'] = dict()
        book_duplicate['similar_bookfields'] = similar_fields
        for field, field_value in similar_fields.items():
            book_duplicate[field] = field_value
        for bookid, book in books.items():
            if BookField.BOOKNAME.value not in similar_fields.keys():
                book_duplicate[str(bookid) + '_' + BookField.BOOKNAME.value] = str(book[BookField.BOOKNAME.value])
            book_duplicate[str(bookid) + '_' + BookField.ACTIVE.value] = bool(book[BookField.ACTIVE.value])
        book_duplicate['ts'] = datetime.datetime.utcnow().isoformat()
        return book_duplicate

    def add_field_amended(self, bookid, field, current, suggested, book) -> dict():
        '''

        :param bookid:
        :param field:
        :param current:
        :param suggested:
        :param book:
        :return: {  log_field[BookField.BOOKID.value] = int(bookid)
                    log_field['bookfield'] = field
                    log_field['current'] = current
                    log_field['suggested'] = suggested
                    log_field['exchangeid'] = int(book[BookField.EXCHANGEID.value])
                    log_field['bookname'] = str(book[BookField.BOOKNAME.value])
                    log_field['active'] = bool(book[BookField.ACTIVE.value])
                    log_field['ts'] = datetime.datetime.utcnow().isoformat()
                    }
        '''
        field_amended = dict()
        field_amended['change_report'] = LogTypes.BOOKFIELD_AMENDED.value
        self.fields_changes_logs(field_amended, bookid, field, current, suggested, book)
        return field_amended

    def add_field_inserted(self, bookid, field, current, suggested, book) -> dict():
        field_inserted = dict()
        field_inserted['change_report'] = LogTypes.BOOKFIELD_INSERTED.value
        self.fields_changes_logs(field_inserted, bookid, field, current, suggested, book)
        return field_inserted

    def add_field_deleted(self, bookid, field, current, suggested, book) -> dict():
        field_deleted = dict()
        field_deleted['change_report'] = LogTypes.BOOKFIELD_DELETED.value
        self.fields_changes_logs(field_deleted, bookid, field, current, suggested, book)
        return field_deleted

    def add_book_with_missing_fields(self, book: dict(), missing_fields: list()) -> dict():
        book_missing_field = dict()
        book_missing_field['change_report'] = LogTypes.BOOK_MISSING_MANDATORY_BOOKFIELDS.value
        for i in range(len(missing_fields)):
            book_missing_field['missing_field_' + str(i+1)] = missing_fields[i]
        self.books_changes_logs(book_missing_field, book)
        return book_missing_field

    def add_extra_static_data(self, id: int(), value, change_report: str(), modified_value=None):
        extra_static = dict()
        extra_static['change_report'] = change_report
        extra_static['static_id'] = int(id)
        extra_static['static_type'] = change_report.split('_')[0]
        if change_report.split('_')[1] != 'insertion':
            extra_static['old_value'] = value
            if modified_value is not None:
                extra_static['new_value'] = modified_value
        else:
            extra_static['new_value'] = value
        extra_static['ts'] = datetime.datetime.utcnow().isoformat()
        return extra_static

    def log_missing_books(self, missing_books: dict()):
        for book in missing_books.values():
            log = self.add_book_deletion(book)
            self.append_log(log)

        # Formatted logs
        if missing_books:
            self.formatted_log_content[LogTypes.BOOK_DELETION.value] = missing_books

    def log_added_books(self, added_books: dict()):
        for book in added_books.values():
            log = self.add_book_insertion(book)
            self.append_log(log)

        # Formatted logs
        if added_books:
            self.formatted_log_content[LogTypes.BOOK_INSERTION.value] = added_books

    def log_added_book_fields(self, added_book_fields: dict()):
        for bookid, book_fields in added_book_fields.items():
            for field, field_value in book_fields['new_bookfields'].items():
                log = self.add_field_inserted(bookid, field, "None", field_value, book_fields['book'])
                self.append_log(log)

        # Formatted logs
        if added_book_fields:
            self.formatted_log_content[LogTypes.BOOKFIELD_INSERTED.value] = added_book_fields

    def log_substracted_book_fields(self, substracted_book_fields: dict()):
        for bookid, book_fields in substracted_book_fields.items():
            for field, field_value in book_fields['old_bookfields'].items():
                log = self.add_field_deleted(bookid, field, field_value, "None", book_fields['book'])
                self.append_log(log)

        # Formatted logs
        if substracted_book_fields:
            self.formatted_log_content[LogTypes.BOOKFIELD_DELETED.value] = substracted_book_fields

    def log_modified_book_fields(self, modified_book_fields: dict()):
        for bookid, book_fields in modified_book_fields.items():
            for field, field_values in book_fields['bookfields'].items():
                log = self.add_field_amended(bookid, field, field_values['old_value'], field_values['new_value'],
                                              book_fields['book'])
                self.append_log(log)

        # Formatted logs
        if modified_book_fields:
            self.formatted_log_content[LogTypes.BOOKFIELD_AMENDED.value] = modified_book_fields

    def log_extra_statics(self, extra_statics: dict()):
        for change_report, changes in extra_statics.items():
            if type(changes) is dict and len(changes) > 0:
                for id, change in changes.items():
                    if type(change) is dict and 'old_value' in change:
                        log = self.add_extra_static_data(id, change['old_value'],change_report, change['new_value'])
                        self.append_log(log)
                        print(log)
                    else:
                        log = self.add_extra_static_data(id, change, change_report)
                        self.append_log(log)
                        print(log)

        # Formatted logs
        for el in extra_statics:
            if extra_statics[el]:
                self.formatted_log_content[el] = extra_statics[el]

    def log_similarities(self, output_list_of_similarities: list()):
        '''
        TODO
        :param output_list_of_similarities:
        :return:
        '''
        for similarities in output_list_of_similarities:
            on_field_names = similarities['similar_bookfields']
            found_duplicate = similarities['similar_books']
            if len(on_field_names) > 0 and len(found_duplicate) > 0:
                log = self.add_book_duplicate(on_field_names, found_duplicate)
                self.append_log(log)

    def log_books_with_missing_fields(self, books_missing_fields: dict()):
        '''
        TODO
        :param books_missing_fields:
        :return:
        '''
        for missing_fields in books_missing_fields.values():
            log = self.add_book_with_missing_fields(missing_fields['book'], missing_fields['missing_bookfields'])
            self.append_log(log)






