#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author   : linjie
import datetime
import logging

import requests
import re
import random
import time
import json
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from util import mysql_util
from util.mysql_util import MySQLUtil
from spider_pretend import request_headers_shopmsg

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  ###禁止提醒SSL警告

'''
商品相关信息爬虫
'''
class Goods_Spider:
    # 日志配置
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    '''
    获取商品信息
    '''
    def getgoodsmsg(self,url):
        if re.findall(r'(.+?):{1}',url)[0]== 'https':
            shopname = re.search('https://(.*?).tmall', url).group(1)
        else:
            shopname = re.search('http://(.*?).tmall', url).group(1)
        searchurl = 'https://{}.m.tmall.com/shop/shop_auction_search.do?spm=a1z60.7754813.0.0.301755f0pZ1GjU&sort=defaul'.format(
            shopname)
        s=requests.session()
        s.headers.update(request_headers_shopmsg)
        page1=s.get(url=searchurl,verify=False).text
        js=json.loads(page1)
        total_page=int(js['total_page'])
        time.sleep(random.random() * 2)
        logging.info('总页数:{}'.format(total_page))
        #获取当前时间
        nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        '''
        创建以店铺为名称的数据表
        '''
        db = MySQLUtil(mysql_util.mysql_conf)
        db.createTable(shopname)
        db.curclose()
        db.close()
        db = MySQLUtil(mysql_util.mysql_conf)
        ###循环获取相关数据的所有值
        for i in range(1,total_page+1):
            time.sleep(random.random() * 2)
            htmlurl=searchurl+'&p={}'.format(i)
            html=s.get(url=htmlurl,verify=False).text
            shop_id_list = []
            shop_title_list = []
            item_id = re.findall('"item_id":(.*?),"',html)
            title = re.findall('"title":"(.*?)","', html)
            sold = re.findall('"sold":"(.*?)","', html)
            totalSoldQuantity = re.findall('"totalSoldQuantity":(.*?),"', html)
            skuurl = re.findall('"url":"(.*?)","', html)
            price = re.findall('"price":"(.*?)","',html)
            imgurl = re.findall('"img":"(.*?)","',html)

            #data数据格式
            data = {'shop_id': shop_id_list, 'shop_title': shop_title_list, 'item_id': item_id, 'title': title,
                    'sold': sold, 'totalSoldQuantity': totalSoldQuantity, 'skuurl': skuurl, 'price': price, 'imgurl':imgurl}
            print(len(data['item_id']))
            itemlength = len(data['item_id'])
            count = 0
            print(i)
            print(data)
            #在每一类下循环插入数据库
            while count<itemlength:
                logging.info('{0}+{1}'.format(data['item_id'][count],data['title'][count]))
                logging.info('{}'.format(data['skuurl'][count]))
                logging.info('{}'.format(data['imgurl'][count]))
                db.insertgoodsmsg(shopname,data['item_id'][count],data['title'][count],data['sold'][count],
                                  data['totalSoldQuantity'][count],data['skuurl'][count],data['price'][count],
                                  data['imgurl'][count],nowTime,nowTime)
                count+=1
        #关闭游标和与数据库连接
        db.curclose()
        db.close()

if __name__=='__main__':
    type = ['女装', '男装', '计算机书籍', '电脑']
    shopurl = Goods_Spider()
    db = MySQLUtil(mysql_util.mysql_conf)
    for index in type:
        datalist = db.getshopurl(index)
        #print(datalist[1][0])
        for i in datalist:
            #移动端的店铺地址，去掉m
            #print(i[0])
            logging.info('url: {}'.format(i[0]))
            url = i[0]
            df=shopurl.getgoodsmsg(url)
            #print(i)
    db.curclose()
    db.close()