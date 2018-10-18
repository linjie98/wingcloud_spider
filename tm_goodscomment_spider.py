#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/17 下午7:47
# @Author  : linjie
# @File    : tm_goodscomment_spider.py
# @Des     : 天猫商品评论爬虫

import requests
import time
import random
import re
import logging

from util import mysql_util
from util.mysql_util import MySQLUtil
from spider_pretend import request_headers_comment


'''
PC端 天猫商品评论爬虫
商品id
商品评论
评论时间
'''
class tm_comment_spider:
    #日志配置
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')
    #itemid = input("itemid:")

    #评论爬虫
    def spider(self,itemid):
        url = "https://rate.tmall.com/list_detail_rate.htm?itemId={0}&spuId=842179060&sellerId=907782288&order=3&append=0&content=1" \
              "&tagId=&posi=&picture=&" \
              "ua=098%23E1hvB9vnvPOvUvCkvvvvvjiPPLqWzjY8RLs9sj3mPmPWljl8RLzvljtWRFqWAjlW9phvHnQGNVinzYswzv5b7MJgzRjw9HuCdphvmpvUG" \
              "9U4V9v1agwCvvpvCvvv2QhvCvvvMMGCvpvVvmvvvhCvmphvLvA4dQvjEGLIAXZTKFEw9Exrs8TJEcqUAj7Q%2Bul1occ63Wv7rjlEgnLv%" \
              "2B2Kz8Z0vQRAn%2BbyDCwFIAXZTKFEw9Exr08TJnDeDyO2vHd8tvpvIvvvvvhCvvvvvvUEpphvvs9vv9DCvpvQovvmmZhCv2jhvv" \
              "UEpphvWw4yCvv9vvUvQORQH1UyCvvOUvvVvayptvpvhvvvvv8wCvvpvvUmmdphvmpvWrUpGPvC1nLyCvvpvvvvv&isg=AurqQavURICRWchqI2pb1fXnO1CGWGXUUQpYDnSi0z2Kp4lhXeg-xXOVQeVA&needFold=0".format(itemid)


        #天猫评论最多99页
        p =0
        page = p+1
        while(page<10):
            logging.info('爬到第{0}页了，再等等！'.format(page))

            #向url中添加的参数
            t = str(time.time() * 1000).split('.')
            param = {
                'currentPage': page,
                '_ksTS': '{}_{}'.format(str(t[0]), str(t[1])),
                'callback': 'jsonp{}'.format(str(int(t[1]) + 1))
            }

            # 随机休眠
            time.sleep(random.random())
            #获取网页
            response = requests.get(url, params=param, headers=request_headers_comment)
            #返回网页内容
            data = response.text
            logging.info('打印返回的内容 : %s' % data)
            text = []

            #正则匹配
            #comment是评论内容
            #comment_time是评论时间
            comment = re.findall(r'\"rateContent\":\".*?\"', data)
            comment_time = re.findall(r'\"rateDate\":\".*?\"', data)

            #评论
            #评论时间
            for i in range(len(comment)):
                db = MySQLUtil(mysql_util.mysql_conf)
                text.append((comment[i].split("\"")[3], comment_time[i].split("\"")[3]))
                #评论
                commentstr = text[i][0]
                logging.info('%s' % commentstr)
                #评论时间
                times = text[i][1]
                logging.info('%s' % times)
                db.insertdata('tm_comm',itemid, commentstr, times)
            page = page+1

if __name__ == '__main__':
    #test
    itemid = input("itemid:")
    sp = tm_comment_spider()
    sp.spider(itemid)