from pylibs.staticdata.generator.book_field import BookField
from pylibs.staticdata.checker.log_types import LogTypes


''' Specs 

I want to be able to look at the amended fields of the books of interest

'''

class BookChecker:

    def __init__(self, data_old, data_new):
        self.data_old = data_old
        self.data_new = data_new
        # self.books_old = self.data_old['books']
        # self.books_new = self.data_new['books']
        # self.old_instruments = self.data_old['instruments']
        # self.new_instruments = self.data_new['instruments']
        # self.old_symbols = self.data_old['symbols']
        # self.new_symbols = self.data_new['symbols']
        # self.old_exchanges = self.data_old['exchanges']
        # self.new_exchanges = self.data_new['exchanges']

        self.books_old = {book['bookid']: book for book in self.data_old['books']}
        self.books_new = {book['bookid']: book for book in self.data_new['books']}
        self.old_instruments = {el['instrumentid']: el for el in self.data_old['instruments']}
        self.new_instruments = {el['instrumentid']: el for el in self.data_new['instruments']}
        self.old_symbols = {el['symbolid']: el for el in self.data_old['symbols']}
        self.new_symbols = {el['symbolid']: el for el in self.data_new['symbols']}
        self.old_exchanges = {el['exchangeid']: el for el in self.data_old['exchanges']}
        self.new_exchanges = {el['exchangeid']: el for el in self.data_new['exchanges']}

    def get_books_old_ids(self):
        return self.books_old.keys()

    def get_books_old_values(self):
        return self.books_old.values()

    def get_books_old(self) -> dict():
        return self.books_old

    def get_books_new_ids(self):
        return self.books_new.keys()

    def get_books_new_values(self):
        return self.books_new.values()

    def get_books_new(self) -> dict():
        return self.books_new

    def get_book_new_field(self, bookid, field):
        return self.books_new[bookid][field]

    def get_book_old_field(self, bookid, field):
        return self.books_old[bookid][field]

    def get_book_old_by_id(self, bookid):
        return self.books_old[bookid]

    def get_book_new_by_id(self, bookid):
        return self.books_new[bookid]

    def set_books_old(self, bookOld):
        self.books_old = bookOld

    def set_books_new(self, bookNew):
        self.books_new = bookNew

    def check_added_books(self):
        """Check if any new book was created in the new file
        :returns
        added_books = {'bookid_1': newbook_1, 'bookid_2': newbook_2, ... }
        """
        added_book_ids = set(self.books_new.keys()) - set(self.books_old.keys())
        added_books = {}
        if added_book_ids:
            for bookid in added_book_ids:
                added_books[bookid] = self.get_book_new_by_id(bookid)
        return added_books

    def check_added_instruments_ids(self):
        return self.check_added_data(self.new_instruments, self.old_instruments)

    def check_missing_instruments_ids(self):
        return self.check_missing_data(self.new_instruments, self.old_instruments)

    def check_modified_instruments(self):
        return self.check_modified_paired_data(self.new_instruments, self.old_instruments)

    def check_added_symbols_ids(self):
        return self.check_added_data(self.new_symbols, self.old_symbols)

    def check_missing_symbols_ids(self):
        return self.check_missing_data(self.new_symbols, self.old_symbols)

    def check_modified_symbols(self):
        return self.check_modified_paired_data(self.new_symbols, self.old_symbols)

    def check_added_exchanges_ids(self):
        return self.check_added_data(self.new_exchanges, self.old_exchanges)

    def check_missing_exchanges_ids(self):
        return self.check_missing_data(self.new_exchanges, self.old_exchanges)

    def check_modified_exchanges(self):
        return self.check_modified_paired_data(self.new_exchanges, self.old_exchanges)

    def check_all_extra_statics_changes(self):
        extra_statics_changes = dict()
        extra_statics_changes[LogTypes.EXCHANGES_AMENDED.value] = self.check_modified_paired_data(self.new_exchanges, self.old_exchanges)
        extra_statics_changes[LogTypes.EXCHANGES_DELETION.value] = self.check_missing_data(self.new_exchanges, self.old_exchanges)
        extra_statics_changes[LogTypes.EXCHANGES_INSERTION.value] = self.check_added_data(self.new_exchanges, self.old_exchanges)
        extra_statics_changes[LogTypes.INSTRUMENTS_AMENDED.value] = self.check_modified_paired_data(self.new_instruments,
                                                                                              self.old_instruments)
        extra_statics_changes[LogTypes.INSTRUMENTS_DELETION.value] = self.check_missing_data(self.new_instruments,
                                                                                       self.old_instruments)
        extra_statics_changes[LogTypes.INSTRUMENTS_INSERTION.value] = self.check_added_data(self.new_instruments,
                                                                                      self.old_instruments)
        extra_statics_changes[LogTypes.SYMBOLS_AMENDED.value] = self.check_modified_paired_data(self.new_symbols,
                                                                                          self.old_symbols)
        extra_statics_changes[LogTypes.SYMBOLS_DELETION.value] = self.check_missing_data(self.new_symbols, self.old_symbols)
        extra_statics_changes[LogTypes.SYMBOLS_INSERTION.value] = self.check_added_data(self.new_symbols, self.old_symbols)
        return extra_statics_changes

    def check_added_data(self, new_data: dict(), old_data: dict()) -> dict():
        added_ids = set(new_data.keys()) - set(old_data.keys())
        added_items = dict()
        if added_ids:
            for id in added_ids:
                added_items[id] = new_data[id]
        return added_items

    def check_missing_data(self, new_data: dict(), old_data: dict()) -> dict():
        missing_ids = set(old_data.keys()) - set(new_data.keys())
        missing_items = dict()
        if missing_ids:
            for id in missing_ids:
                missing_items[id] = old_data[id]
        return missing_items

    def check_modified_paired_data(self, new_data: dict(), old_data: dict()) -> dict():
        modified_values = dict()
        for new_id, new_value in new_data.items():
            if new_id in old_data.keys():
                if old_data[new_id] != new_value:
                    modified_values[new_id] = dict()
                    modified_values[new_id]['old_value'] = old_data[new_id]
                    modified_values[new_id]['new_value'] = new_value
        return modified_values

    def check_modified_dict_fields(self, new_data: dict(), old_data: dict()) -> dict():
        modified_fields = dict()
        for new_id, new_value in new_data.items():
            if new_id in old_data.keys():
                for field in new_value.keys():
                    if field in old_data[new_id] and old_data[new_id][field] != new_value[field]:
                        modified_fields[new_id] = dict()
                        modified_fields[new_id]['book'] = new_value
                        modified_fields[new_id]['fields'] = dict()
                        modified_fields[new_id]['fields'][field] = dict()
                        modified_fields[new_id]['fields'][field]['old_field'] = old_data[new_id][field]
                        modified_fields[new_id]['fields'][field]['new_field'] = new_value[field]
        return modified_fields

    def check_missing_books(self):
        """Check if any old book_id is missing in the new file"""
        missing_ids = set(self.books_old.keys()) - set(self.books_new.keys())
        missing_books = {}
        if missing_ids:
            for bookid in missing_ids:
                missing_books[bookid] = self.get_book_old_by_id(bookid)
        return missing_books

    def check_added_book_fields(self):
        """Check if, for a given bookid in the new book, a new field got created from the old field.
           If True, logs the field in question
           :returns
           added_book_fields : { bookid : {'book': book_6, 'new_fields' : {'fieldname':field_value...}}"""
        added_book_fields = dict()
        for book_id, book_new in self.get_books_new().items():
            if self.get_books_old().get(book_id): #If bookid is found in book_old
                #Check and logs the added fields
                set_diff = set(book_new.keys()) - set(self.get_book_old_by_id(book_id).keys())
                dict_book_and_fields = dict()
                if set_diff:
                    dict_book_and_fields['book'] = book_new
                    dict_book_and_fields['new_bookfields'] = {field_name: book_new[field_name] for field_name in set_diff}
                    added_book_fields[book_id] = dict_book_and_fields
        return added_book_fields

    def check_substracted_book_fields(self, exception_key=None):
        """Check if, for a given bookid in the new book, a field got substracted from the old book.
           If True, logs the field in question
           substracted_book_fields : { bookid : {'book': book_6, 'old_fields' : {'fieldname':field_value...}}"""
        substracted_book_fields = dict()
        for book_id, book_new in self.get_books_new().items():
            if self.books_old.get(book_id): #If bookid is found in book_old
                book_old = self.books_old.get(book_id)
                #Check and logs the substracted fields
                set_diff = set(book_old.keys()) - set(book_new.keys())
                dict_book_and_fields = dict()
                if set_diff:
                    if set_diff != {exception_key}:
                        print(set_diff)
                        dict_book_and_fields['book'] = book_old
                        dict_book_and_fields['old_bookfields'] = {field_name: book_old[field_name] for field_name in set_diff}
                        substracted_book_fields[book_id] = dict_book_and_fields
        return substracted_book_fields

    def check_modified_book_fields(self):
        """List all, for a given bookid in the new book, the fields that got modified from the old book.
           Log the fields and their values, old and new
           modified_book_fields : { bookid : {'book': book_6, 'fields' : {'fieldname1': {'old_volue': old_value, 'new_volue': new_value}... ,
                                                                          'fieldname2': {'old_volue': old_value, 'new_volue': new_value}... ,}}
        """
        modified_book_fields = dict()
        for book_id, book_new in self.get_books_new().items():
            if self.books_old.get(book_id):  # If bookid is found in book_old
                book_old = self.books_old.get(book_id)
                # Collects the fields in common between old book and new book
                set_intersect = set.intersection(set(book_old.keys()), set(book_new.keys()))
                # For the fields in common, only store different values
                dict_book_and_modified_fields = dict()
                dict_field_old_new = dict()
                if set_intersect:
                    dict_book_and_modified_fields['book'] = book_new
                    dict_field_old_new = {field_name: {'old_value': book_old[field_name], 'new_value':book_new[field_name]} for field_name in
                                                                   set_intersect if
                                                                   book_old[field_name] != book_new[field_name]}
                    if dict_field_old_new:
                        dict_book_and_modified_fields['book'] = book_new
                        dict_book_and_modified_fields['bookfields'] = {k: v for k, v in dict_field_old_new.items()}
                        modified_book_fields[book_id] = dict_book_and_modified_fields
        return modified_book_fields

    def check_book_field_similarities(self, *on_field_names, on_field_name_values_filter=dict()):
        """ 3 bis - An extra check for duplicates : if same book_type_id (0 or 1) and same symbolid
        and same exchange_id, this might be a duplicate, so we can double check those too
        :param on_field_names: tuple of book field names to check
               on_field_values: {field_1:[constraint_1,constraint_2,constraint_3], field_2:[...,...], field_n:[...]}
        :returns
               [{fields: {Exchange: 6, bookname: 'xbtusd.d'}, similiar_books: {6:book_6, 8:book_8, 16: book}},
                fields: {Exchange: 8, bookname: 'xbtusd-1'}, similiar_books: {7:book_6, 1418:book_8, 7986: book},
                {Exchange: 6, bookname: 'xbtusd'}, {bookid:6},
                ]
        """
        book_List = list(self.books_new.values())
        output_list_of_similarities = []
        for n, book in enumerate(book_List):
            filter_condition = all([book[field_name] in field_values for field_name, field_values in
                                    on_field_name_values_filter.items() if field_name in book])
            if filter_condition:
                dico_of_similarity = dict()
                dico_of_similar_books = dict()
                for book_i in book_List[n + 1:]:
                    found_duplicate = [bool(book[field] == book_i[field]) for field in on_field_names if
                                       (field in book and field in book_i)]

                    if all(found_duplicate) and len(found_duplicate) == len(on_field_names):

                        book_id = book.get(BookField.BOOKID.value)
                        book_i_book_id = book_i.get(BookField.BOOKID.value)
                        if not dico_of_similar_books.get(book_id):
                            dico_of_similar_books[book_id] = book
                        dico_of_similar_books[book_i_book_id] = book_i
                if len(dico_of_similar_books) > 0:
                    dico_of_similarity['similar_bookfields'] = {field: book[field] for field in on_field_names}
                    dico_of_similarity['similar_books'] = dico_of_similar_books
                    output_list_of_similarities.append(dico_of_similarity)
        return output_list_of_similarities

    def check_fields_exist_in_new_source(self, *field_list, active=True):
        """
        output = #dico[book]:missing_fields ?
        :param field_list: a tuple of BookFields we want to assert are in the new source file (book_new)
        :param active: allows to filter on active books or not
        :return: {6: {'book':book, 'missing_fields': {}}]}
        """
        return_dict = dict()
        for book_id, book_new in self.books_new.items():
            if BookField.ACTIVE.value in book_new and book_new[BookField.ACTIVE.value] == active:
                books_missing_fields = set()
                dict_book_info_i = dict()
                for field in field_list:
                    if field not in book_new:
                        books_missing_fields.add(field)
                if len(books_missing_fields) > 0:
                    dict_book_info_i['book'] = book_new
                    dict_book_info_i['missing_bookfields'] = books_missing_fields
                    return_dict[book_id] = dict_book_info_i
        return return_dict

