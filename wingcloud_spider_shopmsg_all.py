#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author   : linjie

'''
wingcloud_spider获取店铺信息流程
'''
import logging
import sys
import urllib

from tm_shopurl_spider import ShopURL
'''
获取url并存储店铺信息总流程
'''
if __name__ == '__main__':
    #设置类型列表
    type = ['女装', '男装', '计算机书籍', '电脑']
    shoplist = ShopURL()
    for index in type:
        #解决地址栏中 中文编码问题
        typename = urllib.parse.quote(index.encode(sys.stdin.encoding).decode('utf-8'))
        logging.info('type {}'.format(index))
        shoplist.shopslist_spider(typename,index)