# _*_ coding:utf-8 _*_

"""
    该脚本用户修改表每日更新备份状态
    备份地址: https://gist.github.com/diggzhang/ccd56155bf9d037f8e8557237a698658
"""

from pymongo import MongoClient
import pymongo
import sys
import datetime

onion_v4_DB_instance = MongoClient('10.8.8.111', 27017)['eventsV4']
backup_status_collection = onion_v4_DB_instance['backupStatus']
# 为了获取订单表的最新状态,这个脚本只能执行在订单表完全清洗好后
order_events_colllection = onion_v4_DB_instance['orderEvents']

thisDate = datetime.datetime.now() - datetime.timedelta(hours=8)

def main(collectionName, collectionUpdateStatus):
    if list(backup_status_collection.find({"collectionName": collectionName})) == []:
        print("%s 是第一次更新")%(collectionName)
        backup_status_collection.update_one({"collectionName": collectionName}, {"$set": {"state": int(collectionUpdateStatus)}}, upsert=True)
        backup_status_collection.update_one({"collectionName": collectionName}, {"$set": {"lastUpdateTime": thisDate}})
        backup_status_collection.update_one({"collectionName": collectionName}, {"$set": {"updateTime": thisDate}})
    else:
        print("更新 %s 状态为 %s")%(collectionName, collectionUpdateStatus)
        lastUpdateTime = list(backup_status_collection.find({"collectionName": collectionName}))[0]['updateTime']
        backup_status_collection.update_one({"collectionName": collectionName}, {"$set": {"state": int(collectionUpdateStatus)}})
        backup_status_collection.update_one({"collectionName": collectionName}, {"$set": {"lastUpdateTime": lastUpdateTime}})
        backup_status_collection.update_one({"collectionName": collectionName}, {"$set": {"updateTime": thisDate}})

try:
    if len(sys.argv) != 3:
        print("传参错误 arg[0] collection arg[1] status ")
        print("$ python collectionStatus.py <表名字> <状态0/1>")
        print("$ python collectionStatus.py orderEvents 1")
    else:
        collectionName = sys.argv[1]
        collectionUpdateStatus = sys.argv[2]
        main(collectionName, collectionUpdateStatus)

        # 针对orderEvents做特殊处理,查找到orderEvents的最后一条记录时间,并添加到backupStatus表内,需求详情@国杰
        # if collectionName == 'orderEvents':
        #    last_order_event_doc_orderTime = list(order_events_colllection.find({"orderTime": {"$exists": True}}).sort('orderTime', pymongo.DESCENDING))[:1][0]['orderTime']
        #   backup_status_collection.update_one({"collectionName": collectionName}, {"$set": {"lastOrderTime": last_order_event_doc_orderTime}})
except:
    raise
