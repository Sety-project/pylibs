from collections import defaultdict
from pylibs.staticdata.checker.book_checker import BookChecker
from pylibs.staticdata.generator.book_field import BookField
OLD_BOOKS = {"1": {
        BookField.BOOKNAME.value: "XBTUSD_1",
        BookField.BOOKID.value: 1,
        BookField.EXCHANGEID.value: 1,
        BookField.BOOK_TYPE_ID.value: 1,
        BookField.SYMBOLID.value: 1,
        BookField.ACTIVE.value: True
    },
    "2": {
        BookField.BOOKNAME.value: "XBTUSD_1",
        BookField.BOOKID.value: 2,
        BookField.EXCHANGEID.value: 1,
        BookField.BOOK_TYPE_ID.value: 1,
        BookField.SYMBOLID.value: 1,
        BookField.SYMBOL.value: "BTC/USD",
        BookField.ACTIVE.value: True
    },
    "3": {
        BookField.BOOKNAME.value: "XBTUSD_1",
        BookField.BOOKID.value: 3,
        BookField.EXCHANGEID.value: 1,
        BookField.BOOK_TYPE_ID.value: 1,
        BookField.SYMBOLID.value: 1,
        BookField.ACTIVE.value: True

    },
    "4": {
        BookField.BOOKNAME.value: "XBTUSD_1",
        BookField.BOOKID.value: 3,
        BookField.EXCHANGEID.value: 1,
        BookField.BOOK_TYPE_ID.value: 1,
        BookField.SYMBOLID.value: 1,
        BookField.ACTIVE.value: True
    }
}

NEW_BOOKS = {"1": {
        BookField.BOOKNAME.value: "XBTUSD_1",
        BookField.BOOKID.value: 1,
        BookField.EXCHANGEID.value: 10,
        BookField.BOOK_TYPE_ID.value: 1,
        BookField.SYMBOLID.value: 1,
        BookField.ACTIVE.value: True
    }   ,
    "2": {
        BookField.BOOKNAME.value: "XBTUSD",
        BookField.BOOKID.value: 2,
        BookField.EXCHANGEID.value: 1,
        BookField.BOOK_TYPE_ID.value: 1,
        BookField.SYMBOLID.value: 1,
        BookField.UNDERLYING_BOOKID.value: 3,
        BookField.ACTIVE.value: True
    },
    "4": {
        BookField.BOOKNAME.value: "XBTUSD",
        BookField.BOOKID.value: 4,
        BookField.EXCHANGEID.value: 1,
        BookField.BOOK_TYPE_ID.value: 1,
        BookField.SYMBOLID.value: 1,
        BookField.UNDERLYING_BOOKID.value: 3,
        BookField.ACTIVE.value: True
    }
}
OlD_STATIC = {'books': OLD_BOOKS, 'instruments': dict(),
              'symbols':dict(), 'exchanges':dict()}
NEW_STATIC =  {'books': NEW_BOOKS, 'instruments': dict(),
              'symbols':dict(), 'exchanges':dict()}
testBookChecker = BookChecker(OlD_STATIC, NEW_STATIC)

def test_init():
    assert testBookChecker.get_books_old() == OLD_BOOKS
    assert testBookChecker.get_books_new() == NEW_BOOKS

def test_check_missing_books():
    testBookChecker.set_books_old(OLD_BOOKS)
    testBookChecker.set_books_new(NEW_BOOKS)
    missing_books = testBookChecker.check_missing_books()
    assert missing_books == {"3": testBookChecker.get_book_old_by_id("3")}

def test_check_added_book_fields():
    """added_book_fields: {bookid: {'book': book_6, 'new_bookfields': {'fieldname': field_value...}}"""
    testBookChecker.set_books_old(OLD_BOOKS)
    testBookChecker.set_books_new(NEW_BOOKS)
    added_book_fields = testBookChecker.check_added_book_fields()
    assert added_book_fields == {"2": {'book': NEW_BOOKS["2"], 'new_bookfields': {BookField.UNDERLYING_BOOKID.value: 3}},
                                 "4": {'book': NEW_BOOKS["4"], 'new_bookfields': {BookField.UNDERLYING_BOOKID.value: 3}}}

def test_check_modified_book_fields():

    modified_book_field_dico_by_bookid = {'1': {'book': NEW_BOOKS["1"], BookField.EXCHANGEID.value: {'old_value': OLD_BOOKS["1"][BookField.BOOKID.value], 'new_value': NEW_BOOKS["1"][BookField.BOOKID.value]}},
                                          '2': {'book': NEW_BOOKS["2"], BookField.EXCHANGEID.value: {'old_value': OLD_BOOKS["1"][BookField.BOOKID.value], 'new_value': NEW_BOOKS["1"][BookField.BOOKID.value]},
                                                                        BookField.BOOKNAME.value: {'old_value': OLD_BOOKS["2"][BookField.BOOKNAME.value], 'new_value': NEW_BOOKS["2"][BookField.BOOKNAME.value]}
                                                },
                                          '4': {'book': NEW_BOOKS["4"], BookField.BOOKNAME.value: {'old_value': OLD_BOOKS["4"][BookField.BOOKNAME.value], 'new_value': NEW_BOOKS["4"][BookField.BOOKNAME.value]},
                                                                        BookField.BOOKID.value: {'old_value': OLD_BOOKS["4"][BookField.BOOKID.value], 'new_value': NEW_BOOKS["4"][BookField.BOOKID.value]}
                                                }
                                          }

    testBookChecker.set_books_old(OLD_BOOKS)
    testBookChecker.set_books_new(NEW_BOOKS)
    modified_book_list_to_check = testBookChecker.check_modified_book_fields()
    #Compare the keys and the values
    diffkeys = modified_book_list_to_check.keys() != modified_book_field_dico_by_bookid.keys()
    diffvalues = [k for k, v in modified_book_list_to_check.items() if set(v) != set(modified_book_field_dico_by_bookid[k])]
    #Assert no diff
    assert not (diffvalues and diffkeys)

def test_check_added_books():
    testBookChecker.set_books_old(OLD_BOOKS)
    testBookChecker.set_books_new(NEW_BOOKS)
    added_books_to_test = testBookChecker.check_added_books()
    assert list(added_books_to_test.keys()) == list()

    testBookChecker.set_books_old({"1": OLD_BOOKS["1"], "2": OLD_BOOKS["2"], "3": OLD_BOOKS["3"]})
    testBookChecker.set_books_new({"4": NEW_BOOKS["4"]})
    added_books_to_test = testBookChecker.check_added_books()
    assert list(added_books_to_test.keys()) == list(["4"])

    assert added_books_to_test == {'4': NEW_BOOKS["4"]}


def test_check_substracted_book_fields():
    substracted_book_fields_test = {"2": {'book': OLD_BOOKS["2"], 'old_bookfields': {BookField.SYMBOL.value: "BTC/USD"}}}
    testBookChecker.set_books_old(OLD_BOOKS)
    testBookChecker.set_books_new(NEW_BOOKS)
    substracted_book_fields_to_test = testBookChecker.check_substracted_book_fields()
    assert substracted_book_fields_test == substracted_book_fields_to_test

def test_check_book_field_similarities():
    similar_bookfields_test = (BookField.BOOKNAME.value, BookField.EXCHANGEID.value)
    similar_book1 = NEW_BOOKS["2"]
    similar_book2 = NEW_BOOKS["4"]
    check_book_field_similarities_test = [{"similar_bookfields": {BookField.BOOKNAME.value: "XBTUSD", BookField.EXCHANGEID.value: 1},
                                           "similar_books": {2: similar_book1, 4: similar_book2}}]
    testBookChecker = BookChecker(OlD_STATIC, NEW_STATIC)
    testBookChecker.set_books_new(NEW_BOOKS)
    check_book_field_similarities_to_test = testBookChecker.check_book_field_similarities(*similar_bookfields_test)
    assert check_book_field_similarities_test == check_book_field_similarities_to_test

def test_check_fields_exist_in_new_source():
    """
    The test function returns the following format {6: {'book':book, 'missing_fields': {}}]}
    """
    test_field_list = (BookField.BOOKNAME.value, BookField.MIN_ORDER_PRICE.value, BookField.BOOKID.value, BookField.FEE_INSTRUMENT.value)
    test_book_with_missing_fields = NEW_BOOKS["1"]
    fields_exist_in_new_source_test = {"1": {'book': test_book_with_missing_fields,
                                             'missing_bookfields': {BookField.MIN_ORDER_PRICE.value, BookField.FEE_INSTRUMENT.value}
                                             }
                                       }
    testBookChecker.set_books_new({"1": test_book_with_missing_fields})
    fields_exist_in_new_source_to_test = testBookChecker.check_fields_exist_in_new_source(*test_field_list)

    assert fields_exist_in_new_source_test == fields_exist_in_new_source_to_test



