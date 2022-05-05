from pylibs.staticdata.generator.cex_drivers.master_driver import Driver
from pylibs.utils.time.epoch_converter import epoch_to_UTC_ISO
from pylibs.staticdata.generator.book_field import BookField
from pylibs.staticdata.generator.book_type_map import book_types_map, BookTypes
import ccxt
ccxt_map_inv = {'bitmex': 1,
 'bitfinex2': 2,
 'coinbasepro': 3,
 'binance': 4,
 'poloniex': 5,
 'kraken': 6,
 'itbit': 7,
 'bittrex': 8,
 'bitstamp': 9,
 'gemini': 10,
 #'hitbtc': 11,
 #12 : 'krakenrest',
 'binanceus': 13,
 #14 : 'Binanceje',
 #15: 'binance',
 'exx': 16,
 'huobipro': 17,
 #18 : huobi_DM,
 'okcoin': 19,
# 'okex': 20,
#21 : "coinbene",
 #'bybit': 22,
 #'ftx': 23,
 'bitcoincom':24,
 'ascendex': 25,
 #'deribit': 26,
 'phemex': 27,
 'upbit': 28,
'lbank': 35}

class CcxtPublic(Driver):
    def __init__(self, exchange):
        exec('self.connection = ccxt.{}()'.format(exchange))
        self.books = self.connection.load_markets()
        self.exchange = exchange
        self.precisionMode = self.connection.precisionMode
        self.exchangeid = ccxt_map_inv[exchange]
        self.daily_pillars_nb = 3
        self.utc_hours = [ele * 24 / self.daily_pillars_nb for ele in range(0, self.daily_pillars_nb)]
        self.funding_pillars = [int(ele * 3600) for ele in self.utc_hours]

    def get_rawdata(self, book):
        return book['info']

    def get_exchangename(self):
        return self.exchange

    def get_exchange_type(self, book):
         if 'type' in book:
            return self.singular(book['type'])
         else :
            return None

    def get_bookname(self, book):
        if self.exchange == 'kraken':
            if '.d' not in book['id']:
                return  book['info']['wsname']
            else:
                return book['id']
        else :
            return book['id']

    def get_books(self):
        return self.books.values()

    def calculate_increment(self, precision:int()):
        return 1.0/10**precision

    def calculate_precision(self, precision:float()):
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

    def find_precision_with_ccxtMode(self, precision:float()):
        if self.precisionMode == 2: #DECIMAL_PLACES
            return int(precision)
        elif self.precisionMode == 3: #SIGNIFICANT_DIGITS
            return int(precision)
        else:
           return self.calculate_precision(precision)

    def get_proposed_book(self, book):
        proposed_book = {BookField.BOOKNAME.value: self.get_bookname(book),
                         BookField.BASE.value: book['base'],
                         BookField.QUOTE.value: book['quote']}
        if 'precision' in book and 'price' in book['precision'] and book['precision']['price'] is not None:
            proposed_book[BookField.PRICE_PRECISION.value] = self.find_precision_with_ccxtMode(book['precision']['price'])
        if 'precision' in book and 'amount' in book['precision'] and book['precision']['amount'] is not None:
            proposed_book[BookField.QTY_PRECISION.value] = self.find_precision_with_ccxtMode(book['precision']['amount'])
        if 'active' in book.keys():
            proposed_book[BookField.ACTIVE.value] = book['active']
        if 'type' in book.keys():
            if self.get_exchange_type(book) == 'spot':
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
            if self.get_exchange_type(book) == 'option':
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.OPTION.value
        if 'limits' in book:
            if book['limits']['amount']['min'] is not None:
                proposed_book[BookField.QTY_INCREMENT.value] = float(book['limits']['amount']['min'])
              # 2: 'BitfinexCashDerivs' #tBTCUSD = trading #fUSD = funding looks like cash trading with margin and funding enabled
            if self.get_exchangename() == 'bitfinex2':
                proposed_book[BookField.TICK_SIZE.value] = self.calculate_increment(float(book['info']['price_precision']))
                proposed_book[BookField.INITIAL_MARGIN.value] = float(book['info']['initial_margin'])
                proposed_book[BookField.MAINTENANCE_MARGIN.value] = float(book['info']['minimum_margin'])
                proposed_book[BookField.MARGIN_INSTRUMENT.value] = proposed_book[BookField.QUOTE.value]
                proposed_book[BookField.QTY_INCREMENT.value] = float(book['info']['minimum_order_size'])
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                if 'F0' in self.get_bookname(book):
                    book[BookField.BASE.value] = str(book['base']).split('F0')[0]
                    book[BookField.FUNDING_PILLARS.value] = self.funding_pillars
                    if book['quote'] == "USTF0":
                        proposed_book[BookField.QUOTE.value] = 'USDT'
                    else:
                        proposed_book[BookField.QUOTE.value] = book['quote'].split('F0')[0]
                    proposed_book[BookField.BOOK_TYPE.value] = BookTypes.LINEAR_SWAP.value
                    proposed_book[BookField.SETTLEMENT_INSTRUMENT.value] = proposed_book[BookField.BASE.value]
                    proposed_book[BookField.CONTRACT_SIZE] = int(1)
                    proposed_book[BookField.UNIT_SIZE] = float(1)
                    proposed_book[BookField.STD_MULTIPLIER.value] = int(1)
                    proposed_book[BookField.UNIT_INSTRUMENT.value] = proposed_book[BookField.BASE.value]
                    proposed_book[BookField.UNDERLYING_EXCHANGEID.value] = self.exchangeid
                    proposed_book[BookField.UNDERLYING_BOOKNAME.value] = 't' + proposed_book[BookField.BASE.value] + \
                                                                         proposed_book[BookField.QUOTE.value]
            # 3: 'CoinbaseproCash:
            if self.get_exchangename() == 'coinbasepro':
                proposed_book[BookField.TICK_SIZE.value] = book['limits']['price']['min']
                proposed_book[BookField.BOOK_TYPE.value] = 'spot'#no type was there
                proposed_book[BookField.ACTIVE.value] = True
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
            #4: 'BinanceCash'
            if self.get_exchangename() == 'binance':#type good
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['quote']
                if 'tickSize' in book:
                    proposed_book[BookField.TICK_SIZE.value] = book['info']['tickSize']
                elif 'tickSize' in book['info']['filters'][0]:
                    proposed_book[BookField.TICK_SIZE.value] = book['info']['filters'][0]['tickSize']
            #5: 'PoloniexCash'
            if self.get_exchangename() == 'poloniex':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.PASSIVE_FEE_BPS.value] = book['maker']*10000
                proposed_book[BookField.ACTIVE_FEE_BPS.value] = book['taker']*10000
                proposed_book[BookField.TICK_SIZE.value] = book['limits']['price']['min']
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.ACTIVE.value] = True
            # 6: 'KrakenCash' # spot only
            if self.get_exchangename() == 'kraken':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['quote']
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.TICK_SIZE.value] = self.calculate_increment(proposed_book[BookField.PRICE_PRECISION.value]) 
                proposed_book[BookField.CCXT_ID.value] = book['id']
                proposed_book[BookField.ACTIVE.value] = True
                if '.d' in self.get_bookname(book):
                    proposed_book[BookField.IS_DARK_POOL.value] = True
                    proposed_book[BookField.ACTIVE.value] = False
            # 7: 'ItbitCash
            if self.get_exchangename() == 'itbit':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.QTY_INCREMENT.value] = 0.01
                proposed_book[BookField.TICK_SIZE.value] = 0.01 #book['limits']['price']['min']
                proposed_book[BookField.PRICE_PRECISION.value] = 2
                proposed_book[BookField.QTY_PRECISION.value] = 2
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.ACTIVE_FEE_BPS.value] = 35
                proposed_book[BookField.PASSIVE_FEE_BPS.value] = -3
                proposed_book[BookField.ACTIVE.value] = True
            #8: 'BittrexCash
            if self.get_exchangename()== 'bittrex':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.TICK_SIZE.value] = book['limits']['price']['min']
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.ACTIVE.value] = True
            #9: 'BitstampCash
            if self.get_exchangename() == 'bitstamp':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.TICK_SIZE.value] = book['limits']['price']['min']
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.ACTIVE.value] = True
            #10: 'GeminiCash'
            if self.get_exchangename() == 'gemini':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.TICK_SIZE.value] = book['precision']['price']
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.ACTIVE.value] = True
            # 13: 'binanceusCash'
            if self.get_exchangename() == 'binanceus':#type good
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                if 'tickSize' in book:
                    proposed_book[BookField.ACTIVE.value] = book['info']['tickSize']
                else:
                    proposed_book[BookField.TICK_SIZE.value] = book['info']['filters'][0]['tickSize']
            # 16: 'exxCash'
            if self.get_exchangename() == 'exx':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.TICK_SIZE.value] = book['limits']['price']['min']
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.ACTIVE_FEE_BPS.value] = book['taker']*10000
                proposed_book[BookField.PASSIVE_FEE_BPS.value] = book['maker']*10000
                proposed_book[BookField.ACTIVE] = True
            #17: 'huobiGlobalCash'
            if self.get_exchangename() == 'huobipro':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.TICK_SIZE.value] = book['limits']['price']['min']
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.ACTIVE.value] = True
            #19: 'okcoinCash'
            if self.get_exchangename() == 'okcoin':#type good
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.TICK_SIZE.value] = book['info']['tick_size']
            # 24: 'bitcoincom'
            if self.get_exchangename() == 'bitcoincom':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.TICK_SIZE.value] = float(book['info']['tickSize'])
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['info']['feeCurrency']
                proposed_book[BookField.ACTIVE_FEE_BPS.value] = book['taker']*10000
                proposed_book[BookField.PASSIVE_FEE_BPS.value] = book['maker']*10000
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.ACTIVE.value] = True
            #25: 'ascendex'/BITMAXCashDerivs
            if self.get_exchangename() == 'ascendex':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                proposed_book[BookField.ACTIVE.value] = book['active']
                proposed_book[BookField.TICK_SIZE.value] = book['info']['tickSize']
                proposed_book[BookField.ACTIVE_FEE_BPS.value] = book['taker']*10000
                proposed_book[BookField.PASSIVE_FEE_BPS.value] = book['maker']*10000
                proposed_book[BookField.QTY_INCREMENT.value] = float(1)
                if 'PERP' in self.get_bookname(book):
                    proposed_book[BookField.UNDERLYING_EXCHANGEID.value] = self.exchangeid
                    proposed_book[BookField.BOOK_TYPE.value] = BookTypes.LINEAR_SWAP.value
                    proposed_book[BookField.CONTRACT_SIZE.value] = int(1)
                    proposed_book[BookField.UNIT_SIZE.value] = float(book['info']['lotSize'])
                    proposed_book[BookField.UNIT_INSTRUMENT.value] = book['base']
                    proposed_book[BookField.SETTLEMENT_INSTRUMENT.value] = book['quote']
                    proposed_book[BookField.MARGIN_INSTRUMENT.value] = book['quote']
                    proposed_book[BookField.ACTIVE.value] = True
                    proposed_book[BookField.STD_MULTIPLIER.value] = int(1)
                    book[BookField.FUNDING_PILLARS.value] = self.funding_pillars
            #27: 'phemexCashDerivs'
            if self.get_exchangename() == 'phemex':
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
                if self.get_exchange_type(book) == 'perpetual':
                    book[BookField.FUNDING_PILLARS.value] = self.funding_pillars
                    proposed_book[BookField.QTY_INCREMENT.value] = float(1)
                    proposed_book[BookField.UNIT_SIZE.value] = float(book['info']['contractSize'].split(' ')[0])
                    proposed_book[BookField.SETTLEMENT_INSTRUMENT.value] = book['info']['settleCurrency']
                    proposed_book[BookField.MARGIN_INSTRUMENT.value] = book['info']['settleCurrency']
                    proposed_book[BookField.TICK_SIZE.value] = float(book['info']['tickSize'])
                    proposed_book[BookField.CONTRACT_SIZE.value] = int(book['info']['lotSize'])
                    proposed_book[BookField.INITIAL_MARGIN.value] = float(str(book['info']['riskLimits'][0]['initialMargin'])[:-1])/100
                    proposed_book[BookField.MAINTENANCE_MARGIN.value] = float(str(book['info']['riskLimits'][0]['maintenanceMargin'])[:-1])/100
                    proposed_book[BookField.UNDERLYING_BOOKNAME.value] = book['info']['indexSymbol']
                    proposed_book[BookField.UNDERLYING_EXCHANGEID.value] = self.exchangeid
                    proposed_book[BookField.STD_MULTIPLIER.value] = int(book['info']['lotSize'])
                    if book['inverse'] is True:
                        proposed_book[BookField.BOOK_TYPE.value] = BookTypes.INVERSE_SWAP.value
                        proposed_book[BookField.UNIT_INSTRUMENT.value] = book['quote']
                    elif book['linear'] is True:
                        proposed_book[BookField.BOOK_TYPE.value] = BookTypes.LINEAR_SWAP.value
                        proposed_book[BookField.UNIT_INSTRUMENT.value] = book['base']
                    proposed_book[BookField.ACTIVE.value] = True
                else :
                    proposed_book[BookField.TICK_SIZE.value] = book['limits']['price']['min']
            # 28: 'UpbitCash'
            if self.get_exchangename() == 'upbit':
                proposed_book[BookField.TICK_SIZE.value] = book['limits']['price']['min']
                proposed_book[BookField.BOOK_TYPE.value] = BookTypes.SPOT.value
                proposed_book[BookField.ACTIVE.value] = True
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
            # 35: 'lbankCash'
            if self.get_exchangename() == 'lbank':
                proposed_book[BookField.TICK_SIZE.value] = book['limits']['price']['min']
                proposed_book[BookField.BOOK_TYPE.value] = 'spot'
                proposed_book[BookField.ACTIVE.value] = True
                proposed_book[BookField.FEE_INSTRUMENT.value] = book['base']
            if 'e-' in str(proposed_book[BookField.TICK_SIZE.value]):
                proposed_book[BookField.TICK_SIZE.value] = float(("%.17f" % float(proposed_book[BookField.TICK_SIZE])).rstrip('0').rstrip('.'))
        else:
            proposed_book[BookField.ACTIVE.value] = False

        return self.reformated(proposed_book)



