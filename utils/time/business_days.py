import datetime
import pandas as pd

def get_first_day_of_the_week(my_date=datetime.date.today()):
    return my_date - datetime.timedelta(days=my_date.weekday())

def get_first_day_of_month(my_date=datetime.date.today()):
    return my_date.replace(day=1)

def get_first_day_of_the_quarter(my_date=datetime.date.today()):
    return datetime.date(my_date.year, 3 * ((my_date.month - 1) // 3) + 1, 1)

def get_first_day_of_the_year(my_date=datetime.date.today()):
    return my_date.replace(month=1, day=1)

def get_dates_between_start_end(start_date, end_date):
    dates = pd.date_range(start_date, end_date, freq='d')
    dates = [str(d) for d in dates]
    dates = [datetime.date(int(str(d)[0:4]), int(str(d)[5:7]), int(str(d)[8:10])) for d in dates]
    return dates



