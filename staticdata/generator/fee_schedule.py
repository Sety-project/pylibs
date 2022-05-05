class FeeSchedule():
    def ___init___(self):
        self.exchangeid = None
        self.exchange_fees = {}

    def exchange_fees(self):
        return self.exchange_fees
    def book_type_fees(self,book_type):
        return {}
    def book_fees(self, book_type):
        return {}
    def add_book_type_override(self, book_type):
        return
    def add_book_override(self, book):
        return
class BitmexCashFees(FeeSchedule):
    def ___init___(self):
        self.exchangeid = None
        self.exchange_fees = {}





