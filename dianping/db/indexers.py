#!/usr/bin/env python
# -*- coding:utf-8 -*- 
# Author: ZhuGuangfei

"""
 两张表的基本操作类
"""
import pymongo
import time, datetime
import pandas as pd
from pandas import Series, DataFrame

# from dianping.utils import get_first_day_of_month

MONGO_HOST = 'localhost'
# MONGO_HOST = "47.106.103.85"  # 阿里云
MONGO_PORT = 27017  # 端口号
MONGO_DB = "dianping"  # 库名
MONGO_COLL_SHOP = "shop"  # collection名
MONGO_COLL_COMMENT = "comment"  # collection名
MONGO_COLL_URL = 'url'  # 去重的url


class BaseIndexer(object):
    def __init__(self, collection):
        # 链接数据库
        self.client = pymongo.MongoClient(MONGO_HOST, port=MONGO_PORT)
        # 数据库登录需要帐号密码的话
        # self.client.admin.authenticate(settings['MINGO_USER'], settings['MONGO_PSW'])
        self.db = self.client[MONGO_DB]  # 获得数据库的句柄
        self.collection = self.db[collection]

    def get_info_list(self, page_size, page_index, filter_dict, sort_field, deceding_order=True):
        """
        获取表信息
        :param page_size:
        :param page_index:
        :param filter_dict:
        :param sort_field:
        :param deceding_order:
        :return:
        """
        if deceding_order:
            order = pymongo.DESCENDING
        else:
            order = pymongo.ASCENDING
        skip = page_size * (page_index - 1)
        total_count = self.collection.find(filter_dict, projection={'_id': 0}).count()
        results = self.collection.find(filter_dict, projection={'_id': 0}).sort([(sort_field, order)]).limit(
            page_size).skip(skip)
        data_list = [item for item in results]
        return data_list, total_count

    # def get_key_info(self, filter_dict, group_key_list):
    #     """获取分类字段"""
    #     match = {
    #         'update_time': {'$gte': get_first_day_of_month(), '$lte': get_first_day_of_month() + 30 * 24 * 60 * 60}}
    #     match.update(filter_dict)
    #     print('match', match)
    #     group = {'_id': {key: ("$%s" % key) for key in group_key_list}, 'count': {'$sum': 1}}
    #     print('gropu', group)
    #     result = self.collection.aggregate(
    #         [
    #             {'$match': match},
    #             {'$group': group}
    #         ]
    #     )
    #     return result

    def get_all_data_list(self, filter_dict):
        """
        获取所有的数据
        :return:
        """
        data_list = self.collection.find(filter_dict)
        return data_list


indexer = BaseIndexer(MONGO_COLL_SHOP)


# #res = indexer.get_key_info({},['rigin_code','district_code','dish_type_code'])
# res = indexer.get_key_info({},['district_code'])
# res_list,cc = indexer.get_info_list(200,1,{},'update_time')
# for item in res:
#     print(item)



def get_month_time_range(month=None, year=None):
    """
    返回指定月份的第一天和最后一天时间戳
    :param month: 月份 int
    :param year: 年份 int
    :return: (该月第一天第一刻的时间戳，该月最后一天最后一刻的时间戳)
    """
    now_time = datetime.datetime.now()
    if not month:
        month = now_time.month
    if not year:
        year = now_time.year
    if month == 12:
        month = 0
    future_mouth_first = datetime.datetime(year, month + 1, 1, 23, 59, 59)  # 下个月第一天最后一秒
    # 当月最后一天最后一秒时间
    this_month_last_time = future_mouth_first - datetime.timedelta(days=1)  # 字符串格式的时间
    this_month_first_time = datetime.datetime(year, month, 1, 0, 0, 0)  # 字符串格式的时间
    return time.mktime(this_month_first_time.timetuple()), time.mktime(this_month_last_time.timetuple())


begin_of_this_month, end_of_this_month = get_month_time_range()
filter_dict = {'update_time': {'$gte': begin_of_this_month, '$lte': end_of_this_month}}
print(filter_dict)
data_list = list(indexer.get_all_data_list(filter_dict))
shop_data = DataFrame(data_list)
print(shop_data[:5])

# 获取分类
shop_data['rigin_code'].unique()

# 按条件聚合 ---> 计算 数目   # 各区（各品类）店铺数量
shop_data.groupby(['rigin_code', 'dish_type_code']).count().sort_index()

# 将mongodb中的字符串格式的数据转换成数字
shop_data[['flavor_score', 'shop_comment_num', 'service_score', 'shop_rank_score', 'environment_score']] = shop_data[
    ['flavor_score', 'shop_comment_num', 'service_score', 'shop_rank_score', 'environment_score']].apply(pd.to_numeric)

# 按照聚合条件求平均值
shop_data.groupby(['rigin_code', 'dish_type_code']).mean()


