#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: ZhuGuangfei
import datetime
import time
def get_first_day_of_month():
    """
    获取本月的第一天 timestamp
    :return:
    """
    date_time = datetime.date(datetime.date.today().year, datetime.date.today().month, 1).timetuple()
    return time.mktime(date_time)