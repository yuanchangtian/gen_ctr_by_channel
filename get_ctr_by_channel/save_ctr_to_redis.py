
# -*- coding:utf-8 -*-
"""
Created on Tue Dec 26 10:37:55 2017

@author: YuanChangtian
"""
import redis
import datetime

def save_ctr_to_redis(input_path):
    channel_ctr = {}
    key = 'cate_list'
    with open(input_path, 'r') as files:
        for line in files.readlines():
            tmp = line.split('\t')
            channel_ctr[tmp[0]] = float(tmp[1][0:7])
    print channel_ctr
    r = redis.StrictRedis(host='r-2ze1f7d6565dc474.redis.rds.aliyuncs.com', password='jg2xMXzW', port=6379, db=3)
    r.delete(key)
    r.zadd(key,**channel_ctr)
    r.expire(key,86400*5)

if __name__ == '__main__':
    input_path = 'ctr_by_channel.txt'
    save_ctr_to_redis(input_path)
