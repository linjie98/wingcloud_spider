#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author   : linjie
import logging

import requests
import re
import json
from requests.packages.urllib3.exceptions import InsecureRequestWarning

from util import mysql_util
from util.mysql_util import MySQLUtil
from spider_pretend import request_headers_shopmsg #店铺请求头
from util.zh_pin_switch import ZhSwitch

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)  ###禁止提醒SSL警告


#获取店铺基本信息
#店铺id
#店铺名称
#店铺url
class Shops_Spider:
    # 日志配置
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')


    '''
    通过店铺URL获取店铺所有ID
    '''
    def shopmsg(self,url):
        db = MySQLUtil(mysql_util.mysql_conf)
        shopname = re.search('https://(.*?).tmall', url).group(1)
        searchurl = 'https://{}.m.tmall.com/shop/shop_auction_search.do?spm=a1z60.7754813.0.0.301755f0pZ1GjU&sort=defaul'.format(
            shopname)
        s=requests.session()
        s.headers.update(request_headers_shopmsg)
        page1=s.get(url=searchurl,verify=False).text
        #print(page1)
        js=json.loads(page1)
        total_page=int(js['total_page'])
        shop_id=js['shop_id']
        shop_title = js['shop_title']
        logging.info('看看数据有没有{0}'.format(shop_id))
        logging.info('{0}'.format(shop_title))
        logging.info('{0}'.format(url))

        shop_pin_title_obj = ZhSwitch
        shop_pin_title = shop_pin_title_obj.zh_pin(shop_title)

        db.insertshopmsg(shop_id,shop_title,url,shop_pin_title)

        #数据库关闭二连
        db.curclose()
        db.close()

if __name__ == '__main__':
    tm = Shops_Spider()
    tm.shopmsg('https://shiyuemami.tmall.com')