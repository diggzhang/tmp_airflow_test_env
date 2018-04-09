#!/usr/bin/python3
# encoding: utf-8
# https://gist.github.com/diggzhang/dfa80f07715f3d5f3f9047e7ee9480ff

from pymongo import MongoClient
import datetime
import csv
import math
import subprocess
import requests
import json
import logging

users_db_instance = MongoClient('localhost', 27017)['onions']
# users_db_instance = MongoClient('10.8.8.111', 27017)['onions']
users_collection = users_db_instance['users']
# TODO 逐步浮动增加 这里尝试浮动算法后发现没法覆盖随机情况 后一天是前一天5x
DAILYAVG = 180000
users_collection_result_count = 0 #users表初步拉取后量级
today_users_number = 0 #当天用户真实量级
# TODO 报错用大家的dingbot，报喜用自己的
bot_url = 'https://oapi.dingtalk.com/robot/send?access_token=073caf377be9e42501182b3cc755cbd485ded7261a7130a9f66e9a3286c6af91'
bot_back_url = bot_url
logging.basicConfig(filename='daily_users_collection.log',level=logging.DEBUG)

logging.info("++" * 20)
logging.info("开始整理users表 " + str(datetime.datetime.now()))
# Part.1 start ##########################################################
# Remove zombies after users collection restore ready


def dingding_bot(bot_url, data):
    headers = {'Content-Type': 'application/json'}
    post_data = {
        "msgtype": "markdown",
        "markdown": data
    }
    r = requests.post(bot_url, headers=headers,data=json.dumps(post_data))
    logging.info(r.content)


def killProc():
    script_name = __file__
    shell_cmd = "ps -ef | grep " + script_name + " | grep -v grep | awk '{print $2}' | xargs kill -9"
    logging.info("查杀当前users表准备进程，停止后续Oozie任务")
    logging.info("用户表整理失败 " + str(datetime.datetime.now()))
    logging.info("--" * 20)
    subprocess.call(shell_cmd, shell=True)

# users collection count after restore ready
users_collection_result_count = users_collection.count()

zombies_sampling = {
    "registTime": {
	   "$gte": datetime.datetime(2017, 9, 23, 1, 52, 10),
	   "$lt": datetime.datetime(2017, 10, 7)
    },
    "publisher": {'$exists': False},
    "semester": {"$exists": False},
    "school": {"$exists": False},
    "gender": {"$exists": False},
    "channel": "none",
    "from": "pc",
    "type": "signup",
    "scores": 0,
    "points": 0,
    "coins": 0,
    # "phone": {"$regex": "^159.*$"},
    "dailySignIn.times": 0
}

p = users_collection.remove(zombies_sampling)
logging.info('清理僵尸用户数: ' + str(p))
# Part.1 end  ##########################################################

# Part.2 start ##########################################################

today_users_number = users_collection.count()
logging.info("拉取users表后用户量: " + str(users_collection_result_count))
logging.info("清除僵尸用户后users量: " + str(today_users_number))

if p['n'] >= 0 and p['n'] <= 1000:
    text = {
        "title": "users表拉取异常",
        "text": "## users表数据拉取异常\n\n ---- \n\n users表count值 {} \n\n 清理僵尸用户数 {} \n\n 可能线上BSON包准备失败".format(today_users_number, p['n'])
    }
    # dingding_bot(bot_url, text)
    # killProc()

# Part.2 end ##########################################################

# Part.3 start ##########################################################
last_seven_days = None
with open('daily_count.csv', 'rt', encoding="utf-8") as target_csv:
    reader = csv.reader(target_csv)
    last_seven_days = list(reader)
last_seven_days = last_seven_days[0]
logging.info("过去7天users表量级: " + str(last_seven_days))


def get_list_avg(last_seven_days):
    length = len(last_seven_days)
    total = 0
    for value in last_seven_days:
        total = total + int(value)
    return total/length
avg_user_daily_count = get_list_avg(last_seven_days)
logging.info("过去7天平均用户数: " + str(avg_user_daily_count))
# Part.3 end ##########################################################

# Part.4 start ##########################################################

def fineCalc(count, avg):
    diff = abs(math.ceil(count - avg))
    if diff > DAILYAVG:
        logging.info("相比日常差异数据值异常： " + str(diff))
        warning_text = {
            "title": "users表数据异常",
            "text": "## users表数据异常\n\n ---- \n\n users表count值 {} \n\n 7日内均值 {}  \n\n 相较于均值浮动范围 {} \n\n".format(today_users_number, avg_user_daily_count ,diff)
        }
        dingding_bot(bot_url, warning_text)
        killProc()
    else:
        logging.info("相比日常差异数据值正常： " + str(diff))
        text = {
            "title": "users表备份完成",
            "text": "## users表备份完成\n\n ---- \n\n users表count值 {} \n\n 7日内均值 {}  \n\n 相较于均值浮动范围 {} \n\n".format(today_users_number, avg_user_daily_count ,diff)
        }
        dingding_bot(bot_back_url, text)


if today_users_number >= avg_user_daily_count:
    logging.info("今日量级大于平均值" + str(avg_user_daily_count))
    fineCalc(today_users_number, avg_user_daily_count)
else:
    logging.info("今日量级低于平均值" + str(avg_user_daily_count))
    fineCalc(today_users_number, avg_user_daily_count)
# Part.4 end ##########################################################
# Part.5 start ##########################################################
# Update range of avg
last_seven_days.remove(last_seven_days[0])
last_seven_days.append(today_users_number)
with open('daily_count.csv', 'w', encoding="utf-8") as target_csv:
    writer = csv.writer(target_csv)
    writer.writerow(last_seven_days)
# Part.5 end ##########################################################
logging.info("用户表整理完成 " + str(datetime.datetime.now()))
logging.info("--" * 20)
