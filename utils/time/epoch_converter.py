import datetime
from datetime import timezone
import pytz
import calendar

_EPOCH_START = datetime.datetime(1970, 1, 1)
utc_tz = pytz.timezone("UTC")

def convert_utc_to_epoch(dt):
    ''' dt should be a utc_tz.localize(dt)'''
    timestamp = (utc_tz.localize(dt) - utc_tz.localize(_EPOCH_START)).total_seconds()
    return timestamp

def convert_epoch_to_utc(epoch):
    '''
    Function that reverts a converter time from epoch to UTC
    :param epoch: epoch must be in seconds, not ns
    :returns: a UTC datetime like datetime.datetime(2021, 4, 12, 18, 35, 12, 289340, tzinfo=<UTC>)
    '''
    return datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC")) + datetime.timedelta(seconds=epoch)

def get_datetime_day(day, month, year):
    return datetime.date(year, month, day)

def human_time(datetime_pt, date_format="%Y%m%d"):
    '''
    :param datetime_pt: a datetime object
    :param date_format: the string format of the date willing to be returned.
    Could be "%Y-%m-%d %H:%M:%S.%f" for additional precision
    :return: a string for the date in the specified format
    '''
    return datetime_pt.strftime(date_format)

def get_dt_from_humantime(humantime, date_format='%Y%m%d'):
    return datetime.datetime.strptime(humantime, date_format)

def to_ns(datetime_pt):
    return calendar.timegm(datetime_pt.timetuple())*1e9

def now_utc_iso(date_time):
    return datetime.datetime.now(tz=pytz.utc).isoformat(timespec='milliseconds')

def getDateTimeFromISOString(s):
    d = dateutil.parser.parse(s)
    return d

def epoch_to_UTC_ISO(seconds_since_epoch):
    return datetime.datetime.utcfromtimestamp(seconds_since_epoch).isoformat(timespec='milliseconds')

def ns_to_human_time(timens):
    timems = timens/1000
    human_time = datetime.datetime(1970, 1, 1, tzinfo=pytz.timezone("UTC")) + datetime.timedelta(microseconds=timems)
    human_time = str(human_time.hour)+":"+str(human_time.minute)+":"+str(human_time.second)+" "+str(human_time.microsecond)
    return human_time

def dtime_between_ts(ts_1, ts_2):
    dtime = ts_2 - ts_1
    return dtime


# cecile usage example
# naive_dt = datetime.datetime(2021, 4, 14, 0, 0, 0)
# aware_dt = utc_tz.localize(naive_dt)
# epoch = convert_utc_to_epoch(aware_dt)
# print(epoch)
# 
# dt = convert_epoch_to_utc(1618358400)
# print(dt)
#
# day = datetime.date(2021, 4, 12)
# print(to_ns(day))
# print(human_time(day))

# timens = 1618252512289339648
# human_time = ns_to_human_time(timens)
# print(human_time)
#ts_1=1618252512100000000
#ts_2=1618252512400000000
#print(dtime_between_ts(ts_1, ts_2))
