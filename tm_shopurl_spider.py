#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author   : linjie
import logging

import requests
from bs4 import BeautifulSoup
import urllib.request
import urllib.parse
import re
import ssl
from spider_pretend import request_headers_shopurl
import sys

from tm_shopsmsg_spider import Shops_Spider

ssl._create_default_https_context = ssl._create_unverified_context

'''
获取店铺url
'''
class ShopURL:
    # 日志配置
    logging.basicConfig(level=logging.DEBUG,
                        format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s',
                        datefmt='%Y-%m-%d %H:%M:%S')

    #重定向获取真实url
    #重试机制
    def get_shopurl(self,url, try_count=1):

        if try_count > 3:
            return url
        try:
            rs = requests.get(url, headers=request_headers_shopurl, timeout=5)
            if rs.status_code > 400:
                return self.get_shopurl(url, try_count + 1)
            return rs.url
        except:
            return self.get_shopurl(url, try_count + 1)


    def shopslist_spider(self,shoptype,shop_type):
        html_url = "https://list.tmall.com/search_product.htm?spm=a220m.1000858.1000724.7.2be53903j7j3lj&q={}&sort=s&style=w&from=mallfp..pc_1_searchbutton&active=2#J_Filter".format(shoptype)
        req = urllib.request.Request(html_url)
        web_page = urllib.request.urlopen(req)
        html = web_page.read()

        soup = BeautifulSoup(html, 'html.parser')  # 文档对象
        count = 0
        for u in soup.find_all('a',{"class","sHe-shop"}):
            print(u['href'])
            #将获取的url加上https:拼接
            httphead = "https:"
            go_shop_url = httphead+u['href']
            logging.info('未重定向前的url：{}'.format(go_shop_url))
            url = self.get_shopurl(go_shop_url)
            logging.info('未截取前的url：{}'.format(url))
            #http://aaskincare.tmall.com/shop/view_shop.htm?user_number_id=1034143854&rn=b06cb56d6cb59a3fbb42019f7553acd0&tbpm=1
            #匹配前面是com的/符号
            shopurl = re.findall("(.+?)(?<=com)/",url)[0]

            #获取店铺具体信息并插入到mysql中
            tsus = Shops_Spider()
            try:
                tsus.shopmsg(shopurl,shop_type)
            except Exception as e:
                logging.error('{}'.format(e))
            else:
                logging.info('获取店铺具体信息并插入数据到数据库成功')

            logging.info('店铺url：{}'.format(shopurl))
            count+=1
            logging.info('count == {}'.format(count))
            if count == 2:
                break

if __name__ == '__main__':
    a = ShopURL()
    type = ['女装','男装','电脑','化妆品','童装','零食','手机','母婴','汽车','医药','计算机书籍']
    for index in type:
        #解决地址栏中 中文编码问题
        typename = urllib.parse.quote(index.encode(sys.stdin.encoding).decode('utf-8'))
        logging.info('type {}'.format(index))
        a.shopslist_spider(typename)
