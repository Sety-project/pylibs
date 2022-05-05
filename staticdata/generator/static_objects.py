class Book(dict):
   def __init__(self,*arg,**kw):
      super(Book, self).__init__(*arg, **kw)
   def get_bookid(self):
      return self['bookid']
   def is_complete(self):
      return 'bookid' in self.keys()

b = Book()

print(f'is complete: {b.is_complete()}')
b['bookid'] = 1
print(f'is complete: {b.is_complete()}')

print(f"{b['bookid']}")

print(f"{b.get_bookid()}")
