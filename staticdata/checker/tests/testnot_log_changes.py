from pylibs.staticdata.checker.log_changes import LogChanges
from pylibs.staticdata.generator.book_field import BookField
book = {'bookname': 'tBABUSD',
 'bookid': 284,
 'base': 'BCH',
 'quote': 'USD',
 'price_precision': 5,
 'qty_precision': 8,
 'passive_fee_bps': 10.0,
 'active_fee_bps': 20.0,
 'passive_fee_cst': 0.0,
 'active_fee_cst': 0.0,
 'max_order_qty': 500.0,
 'min_order_qty': 0.02,
 'exchange': 'Bitfinex_CS',
 'exchangeid': 2,
 'active': False,
 'book_type_id': 0,
 'base_instrumentid': 129,
 'quote_instrumentid': 2,
 'symbolid': 278,
 'symbol': 'BCH/USD',
 'book_type': 'spot'}
book2 = {'bookname': 'tBABUSD',
 'bookid': 285,
 'base': 'LTC',
 'quote': 'USD',
 'price_precision': 5,
 'qty_precision': 8,
 'passive_fee_bps': 10.0,
 'active_fee_bps': 20.0,
 'passive_fee_cst': 0.0,
 'active_fee_cst': 0.0,
 'max_order_qty': 500.0,
 'min_order_qty': 0.02,
 'exchange': 'Bitfinex_CS',
 'exchangeid': 2,
 'active': False,
 'book_type_id': 0,
 'base_instrumentid': 129,
 'quote_instrumentid': 2,
 'symbolid': 278,
 'symbol': 'BCH/USD',
 'book_type': 'spot'}

lg = LogChanges()
def test_Init_():
    assert len(lg.get_new_book_insertion())  == 0
    assert len(lg.get_book_deletion()) == 0
    assert len(lg.get_fields_amended()) == 0
    assert len(lg.get_log_content()) == 0

def test_add_book_insertion():
    lg.add_book_insertion(book)
    assert lg.add_book_insertion(book)['book'] == book


def test_add_book_deletion():
    lg.add_book_deletion(book)
    assert lg.add_book_deletion(book)[BookField.BOOKNAME.value] == book['bookname']
    assert lg.add_book_deletion(book)[BookField.EXCHANGEID.value] == book['exchangeid']

def test_add_field_amended():
    adds = lg.add_field_amended(123, 'quote', 'USD', 'BTC', book)
    assert adds['current'] == 'USD'
    assert adds['suggested'] == 'BTC'

def test_add_field_inserted():
    adds = lg.add_field_inserted(123, 'quote', 'USD','BTC', book)
    assert adds['current'] == 'USD'
    assert adds['suggested'] == 'BTC'

def test_add_field_deleted():
    adds = lg.add_field_deleted(123, 'quote', 'USD', 'BTC', book)
    assert adds['current'] == 'USD'
    assert adds['suggested'] == 'BTC'

def test_add_book_duplicate():
    similar_fields = {BookField.EXCHANGEID.value: book[BookField.EXCHANGEID.value], BookField.BOOKNAME.value: book[BookField.BOOKNAME.value]}
    dup_log = lg.add_book_duplicate(similar_fields, {book[BookField.BOOKID.value]: book, book2[BookField.BOOKID.value]: book2})
    assert dup_log[BookField.EXCHANGEID.value] == book[BookField.EXCHANGEID.value]


def test_add_book_with_missing_fields():
    missing_fields = [BookField.PRICE_PRECISION.value, BookField.QTY_PRECISION.value]
    dup_log = lg.add_book_with_missing_fields(book, missing_fields)
    assert dup_log['missing_field_1'] == missing_fields[0]
    assert dup_log[BookField.BOOKID.value] == book[BookField.BOOKID.value]

def test_log_modified_book_fields():
    lg.log_modified_book_fields({'1': {"book": book, 'bookfields': {BookField.SYMBOLID.value: {'old_value': 1, 'new_value': 2}}}})

def test_log_substracted_book_fields():
    lg.log_substracted_book_fields({'1': {"book": book, 'old_bookfields': {BookField.SYMBOLID.value: 1}}})

def test_log_added_book_fields():
    lg.log_added_book_fields({'1': {"book": book, 'new_bookfields': {BookField.SYMBOLID.value: 1}}})

def test_log_missing_books():
    lg.log_missing_books({1: book, 2: book2})

def test_log_added_books():
    lg.log_added_books({1: book, 2: book2})

def test_log_books_with_missing_fields():
    lg.log_books_with_missing_fields({'1': {"book": book, 'missing_bookfields': ['price_precision', 'qty_precision']}})

def test_log_similarities():
    lg.log_similarities([{'similar_bookfields': {'price_precision': 0, 'qty_precision': 0}, 'similar_books': {1: book, 2: book2}}])
