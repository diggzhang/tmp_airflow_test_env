#!/usr/bin/python3
# encoding: utf-8
"""
Created on 02/06/2017
@author: Tao

https://gist.github.com/diggzhang/9a0a0c97b47186b601734ea722ab05e5

changelog:
    @diggzhang 20/01/2018: handle `distributionList` fields
    @diggzhang 04/09/2017: add `vipType` filed
    @diggzhang 05/09/2017: change `vipType` filed if `good.vipType` is not exists fill in vip#2-1
    @diggzhang 06/09/2017: change `vipType` in 8yuan order filed if `good.vipType` is not exists fill in vip#2-1
    @diggzhang 26/09/2017: add `goodType`
    @diggzhang 28/10/2017: `isTest` logic error change it enum True False
    @diggzhang 30/10/2017: add `orderId` from _id
    @diggzhang 16/11/2017: add `trasactionId` for refund for stores

doing:
    @diggzhang 29/03/2018: add new goodType `littleclass`

todo:
    @liutao 06/02/2018: add `orders.paymentPlatform`
    @diggzhang goodType package
"""
# _*_ coding:utf-8 _*_
from pymongo import MongoClient
from bson.objectid import ObjectId
from pyqqwry.qqwry import QQWry
from datetime import datetime
import uuid

qq_wry = QQWry("./qqwry.dat")

# TODO: change mongo env as online configure
order_history_db_instance = MongoClient('localhost', 27017)['eventsV4']
event_db_instance = MongoClient('localhost', 27017)['eventsV4']

# order history collection
order_history_collection = order_history_db_instance['orderHistory']
order_origin_collection = order_history_db_instance['orders']
goods_collection = order_history_db_instance['allGoods'] # allGoods存在是为了照顾8元订单
# TODO: change collection name
event_collection = event_db_instance['orderEvents']

event_collection.drop()
allGoodsInfo = list(goods_collection.find({}, {"_id": True, "amount": True, "addVIPTime": True}))


def find_location_by_ip(ip):
    location = qq_wry.query(ip)
    return location[1] or location[0]

def distribution_list_process(doc, eventkey_name, bundle_flag):
    event_obj_list = []
    for item in doc['good']['distributionList']:
        event_obj = {}
        if 'vip' == item['kind']:
            event_obj['actualOrderAddVIPTime'] = item['params']['addTime']
            event_obj['actualOrderAmount'] = doc['good']['amount']
            event_obj.update(doc_public_fileds_parse_bundle(doc, eventkey_name, bundle_flag, item['kind'], item['params']))
            event_obj_list.append(event_obj)
        elif 'package' == item['kind']:
            event_obj['actualOrderAddVIPTime'] = item['params']['addTime']
            event_obj['actualOrderAmount'] = doc['good']['amount']
            event_obj.update(doc_public_fileds_parse_bundle(doc, eventkey_name, bundle_flag, item['kind'], item['params']))
            event_obj_list.append(event_obj)
        elif 'littleClass' == item['kind']:
            # TODO event_obj['actualOrderAddVIPTime'] = item['params']['addTime']
            event_obj['actualOrderAmount'] = doc['good']['amount']
            event_obj.update(doc_public_fileds_parse_bundle(doc, "paymentSuccessLittleClass", bundle_flag, item['kind'], item['params']))
            event_obj_list.append(event_obj)
        else:
            print(doc['_id'])

    return event_obj_list


# 处理绑定订单
def doc_public_fileds_parse_bundle(doc, eventkey_name, bundle_flag, kind, params):
    event_obj = {"category": "order", "platform": "backend", 'eventKey': eventkey_name, 'isBundle': bundle_flag}

    if kind == 'littleClass':
        start = datetime.strptime(params['startTime'].split('T')[0], '%Y-%m-%d')
        end = datetime.strptime(params['endTime'].split('T')[0], '%Y-%m-%d')
        event_obj['actualOrderAddVIPTime'] = (end - start).total_seconds() * 1000

    if len(doc['good']['distributionList']) == 1:
        event_obj['isBundle'] = False

    event_obj['goodType'] = kind

    if params is not None and kind == 'vip':
        event_obj['vipType'] = str('vip#' + params['stage'] + '-' + params['subject'])
    elif kind == 'package':
        event_obj['vipType'] = "vip#2-1"
    elif kind == 'littleClass':
        event_obj['classId'] = params['classId']

    if 'isRenewal' in doc:
        event_obj['isRenewal'] = doc['isRenewal']

    if 'isTest' in doc:
        event_obj['isTest'] = doc['isTest']
    else:
        event_obj['isTest'] = False

    event_obj['user'] = doc['userId']
    event_obj['orderId'] = str(doc['_id'])
    event_obj['orderTime'] = doc["updatedAt"]
    event_obj['serverTime'] = doc["updatedAt"]
    event_obj['orderCreateTime'] = doc["createdAt"]

    if 'originalGood' in doc:
        event_obj['originOrderGoodId'] = doc['originalGood']['_id']
        if doc["originalGood"]["amount"] > 0 and kind != 'package':
            event_obj['originOrderAmount'] = doc['originalGood']['originalAmount']
        elif doc["originalGood"]["amount"] > 0 and kind == 'package':
            event_obj['originOrderAmount'] = doc['originalGood']['amount']

    if 'paymentCredentials' in doc and 'channel' in doc['paymentCredentials'] and 'client_ip' in doc['paymentCredentials']:
        event_obj['channel'] = doc['paymentCredentials']['channel']
        event_obj['ip'] = doc['paymentCredentials']['client_ip']
        event_obj['location'] = find_location_by_ip(doc['paymentCredentials']['client_ip'])
        event_obj['trasactionId'] = doc['paymentCredentials']['order_no']

    if 'creationWay' in doc:
        if 'platform' in doc['creationWay']:
            event_obj['os'] = doc['creationWay']['platform']
        elif 'platform ' in doc['creationWay']:
            event_obj['os'] = doc['creationWay']['platform ']

        if 'onH5' in doc['creationWay']:
            event_obj['onH5'] = doc['creationWay']['onH5']
        elif 'onH5 ' in doc['creationWay']:
            event_obj['onH5'] = doc['creationWay']['onH5 ']

        if 'bySelf' in doc['creationWay']:
            event_obj['bySelf'] = doc['creationWay']['bySelf']
        elif 'bySelf ' in doc['creationWay']:
            event_obj['bySelf'] = doc['creationWay']['bySelf ']

        if 'report' in doc['creationWay']:
            event_obj['report'] = doc['creationWay']['report']
        elif 'report ' in doc['creationWay']:
            event_obj['report'] = doc['creationWay']['report ']

    return event_obj


def doc_public_fileds_parse(doc, eventkey_name, bundle_flag):
    event_obj = {"category": "order", "platform": "backend", 'eventKey': eventkey_name, 'isBundle': bundle_flag}

    if 'isRenewal' in doc:
        event_obj['isRenewal'] = doc['isRenewal']

    if 'isTest' in doc:
        event_obj['isTest'] = doc['isTest']
    else:
        event_obj['isTest'] = False

    event_obj['user'] = doc['userId']
    event_obj['orderId'] = str(doc['_id'])
    event_obj['orderTime'] = doc["updatedAt"]
    event_obj['serverTime'] = doc["updatedAt"]
    event_obj['orderCreateTime'] = doc["createdAt"]

    if 'paymentPlatform' in doc:
        event_obj['paymentPlatform'] = doc['paymentPlatform']

    # for amount 8 order
    if 'good' in doc:
        if type(doc['good']) is ObjectId:
            good_id = doc["good"]
            event_obj['originOrderGoodId'] = good_id
            event_obj['originOrderAmount'] = (item for item in allGoodsInfo if item['_id'] == good_id).next()['amount']
            event_obj['actualOrderAmount'] = event_obj['originOrderAmount']
            event_obj['actualOrderAddVIPTime'] = (item for item in allGoodsInfo if item['_id'] == good_id).next()['addVIPTime']
            event_obj['vipType'] = "vip#2-1"
        elif doc["good"]["amount"] <= 0:
            good_id = doc["good"]["_id"]
            event_obj['originOrderGoodId'] = good_id
            #TODO: event_obj['originOrderAmount'] = (item for item in allGoodsInfo if item['_id'] == good_id).next()['amount']
            event_obj['originOrderAmount'] = 8
            event_obj['actualOrderAmount'] = event_obj['originOrderAmount']
            #TODO event_obj['actualOrderAddVIPTime'] = (item for item in allGoodsInfo if item['_id'] == good_id).next()['addVIPTime']
            event_obj['actualOrderAddVIPTime'] = 0
            event_obj['vipType'] = "vip#2-1"
        else:
            if '_id' in doc['good']:
                event_obj['originOrderGoodId'] = doc['good']['_id']
            if 'amount' in doc['good']:
                event_obj['actualOrderAmount'] = doc['good']['amount']
            if 'addVIPTime' in doc['good']:
                event_obj['actualOrderAddVIPTime'] = doc['good']['addVIPTime']
            if 'vipType' in doc['good']:
                event_obj['vipType'] = doc['good']['vipType']
            else:
                event_obj['vipType'] = "vip#2-1"

    if 'originalGood' in doc:
        event_obj['originOrderGoodId'] = doc['originalGood']['_id']
        if doc["originalGood"]["amount"] > 0:
            event_obj['originOrderAmount'] = doc['originalGood']['amount']

    if 'paymentCredentials' in doc and 'channel' in doc['paymentCredentials'] and 'client_ip' in doc['paymentCredentials']:
        event_obj['channel'] = doc['paymentCredentials']['channel']
        event_obj['ip'] = doc['paymentCredentials']['client_ip']
        event_obj['location'] = find_location_by_ip(doc['paymentCredentials']['client_ip'])
        event_obj['trasactionId'] = doc['paymentCredentials']['order_no']

    if 'creationWay' in doc:
        if 'platform' in doc['creationWay']:
            event_obj['os'] = doc['creationWay']['platform']
        elif 'platform ' in doc['creationWay']:
            event_obj['os'] = doc['creationWay']['platform ']

        if 'onH5' in doc['creationWay']:
            event_obj['onH5'] = doc['creationWay']['onH5']
        elif 'onH5 ' in doc['creationWay']:
            event_obj['onH5'] = doc['creationWay']['onH5 ']

        if 'bySelf' in doc['creationWay']:
            event_obj['bySelf'] = doc['creationWay']['bySelf']
        elif 'bySelf ' in doc['creationWay']:
            event_obj['bySelf'] = doc['creationWay']['bySelf ']

        if 'report' in doc['creationWay']:
            event_obj['report'] = doc['creationWay']['report']
        elif 'report ' in doc['creationWay']:
            event_obj['report'] = doc['creationWay']['report ']

    if 'actualOrderAmount' in event_obj and 'originOrderAmount' in event_obj:
        if event_obj['actualOrderAmount'] == event_obj['originOrderAmount']:
            event_obj['useCoupon'] = False
        else:
            event_obj['useCoupon'] = True

    if 'goodType' in doc:
        event_obj['goodType'] = doc['goodType'] # enum [ "vip", "package" ]
        if doc['goodType'] == "package":
            event_obj['actualOrderAddVIPTime'] = 2592000000.0

    return event_obj


"""
    转码order history
"""


def parseDoc(doc, eventkey_name):
    event_obj = doc_public_fileds_parse(doc['d'], eventkey_name)
    return event_obj


"""
    持久化
"""


def save_eventlist_to_db(event_list):
    event_collection.insert_many(event_list)


"""
    生成event
"""


def generater_events(docs, eventkey_name):
    this_event_list = []
    for doc in docs:
        this_event_list.append(parseDoc(doc, eventkey_name))
    save_eventlist_to_db(this_event_list)


"""
    订单各种状态到eventKey的映射
"""
order_status = [
    # ["创建订单", "createOrder"],
    # ["等待支付", "waitForPayment"],
    # ["分享订单", "shareOrder"],
    # ["请求退款", "refundRequest"],
    # ["退款成功", "refundSuccess"],
    # ["支付超时", "paymentTimeout"],
    # ["支付关闭", "paymentClose"],
    # ["正在退款", "refunding"],
    # ["退款失败", "refundFail"]
]

"""
    Main function:
    find the event stuff then convert to eventKey finally drop this stuff
"""

def parsePaymentSuccessDoc(doc, eventkey_name, bundle_flag):
    if False == bundle_flag:
        event_obj = doc_public_fileds_parse(doc, eventkey_name, bundle_flag)
        return event_obj
    elif True == bundle_flag:
        event_obj_list = distribution_list_process(doc, eventkey_name, bundle_flag)
        return event_obj_list

def generater_payment_bundle_events(docs, eventkey_name, bundle_flag):
    docs_inside = docs
    success_bundle_events_list = []
    for doc in docs_inside:
        event_obj_list = parsePaymentSuccessDoc(doc, eventkey_name, bundle_flag)
        event_collection.insert_many(event_obj_list)

def generater_payment_success_events(docs, eventkey_name, bundle_flag):
    print("need to process %d docs." % len(docs))
    if False == bundle_flag:
        success_events_list = []
        for doc in docs:
            success_events_list.append(parsePaymentSuccessDoc(doc, eventkey_name, bundle_flag))
        save_eventlist_to_db(success_events_list)
    elif True == bundle_flag:
        generater_payment_bundle_events(docs, eventkey_name, bundle_flag)

def payment_success_processing(bundle_flag):
    query_dict = {"status": "支付成功", "good.distributionList": {"$exists": bundle_flag} }
    status_list = list(order_origin_collection.find(query_dict))
    if status_list != [] and status_list is not None:
        generater_payment_success_events(status_list, "paymentSuccess", bundle_flag)
        print("processing %d orders" % len(status_list))
    else:
        print("payment success orders processing error, bundle is %s" % bundle_flag)

# True - 是绑定订单
# ["支付成功","paymentSuccess"] 单独处理旧结构
payment_success_processing(False)
# ["支付成功","paymentSuccess"] 单独处理绑定订单
payment_success_processing(True)

for status in order_status:
    query_dict = {"d.status": status[0]}
    eventkey = status[1]
    status_list = list(order_history_collection.find(query_dict))
    if status_list != [] and status_list != None:
        print("%s %d") % (status[0], len(status_list))
        generater_events(status_list, eventkey)
    else:
        print("%s %d is None") % (status[0], len(status_list))
