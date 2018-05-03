# timezone solution from: https://stackoverflow.com/questions/12165691/python-datetime-with-timezone-to-epoch/17257177#17257177

'''
    Take a timezone in the form of `MM/DD/YYYY HH:MM:SS {AM/PM}` 
    and convert to unix epoch in python3
'''

from datetime import datetime
import pytz


def format_date(dt):
    d = dt.split()[0]
    t = dt.split()[1:]
    MM, DD, YYYY = d.split('/')
    HH, MIN, SS = t[0].split(':')
    if (t[1] == 'PM') and (HH != '12'):
        HH = (int(HH) + 12)

    dtobj = datetime(int(YYYY), int(MM), int(DD), int(HH), int(MIN), int(SS))

    return dtobj

def convert_timestamp(dateIN):
    tz = pytz.timezone('US/Eastern')
    dt_to_convert = format_date(dateIN)

    # a datetime with timezone
    dt_with_tz = tz.localize(dt_to_convert, is_dst=None)

    # get timestamp
    ts = (dt_with_tz - datetime(1970, 1, 1, tzinfo=pytz.utc)).total_seconds()
    
    return int(ts)
