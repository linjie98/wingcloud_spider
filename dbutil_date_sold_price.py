#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @File  : dbutil_date_sold_price.py
# @Author: linjie
# @Date  : 2018/7/16 0016
# @Desc  :
import datetime

import pymysql
import pymongo
import logging
import time
from dbutil1_config import DB_CONFIG

# py连接MongoDB
client = pymongo.MongoClient(host='127.0.0.1', port=27017)

'''
数据库工具类
功能：
    1、日志级别控制
    2、初始化数据库连接参数
    3、获取游标(即数据库连接对象)
    4、查询方法queryOperation
'''

class DbUtil(object):
    # 日志级别：critical > error > warning > info > debug
    # 修改logging的级别,默认是INFO,需要修改成debug(最小级别)才能访问所有级别
    logging.basicConfig(level=logging.DEBUG)

    # 初始化数据库连接参数
    def __init__(self, host, port, user, pwd, dbname):
        # localhost,ip
        self.host = host
        # 端口
        self.port = port
        # 用户名
        self.user = user
        # 密码
        self.pwd = pwd
        # 数据库名称
        self.dbname = dbname

    '''
    获取游标(数据库连接对象)
    '''

    def getCursor(self):
        # 初始化连接
        # self.db = pymysql.connect(self.host,self.port,self.user,self.pwd,self.dbname,charset='utf8')
        self.db = pymysql.connect(host=self.host,
                                  port=self.port,
                                  user=self.user,
                                  password=self.pwd,
                                  db=self.dbname,
                                  charset='utf8mb4',
                                  cursorclass=pymysql.cursors.DictCursor)

        # 获取游标
        cur = self.db.cursor()
        logging.info('游标在下面')
        logging.debug(cur)
        # print('as',cur)
        return cur

    '''
    查询操作
    '''

    def queryOperation(self, sql):
        # 获取数据库游标
        cur = self.getCursor()
        # print("sdasds")
        # 执行查询
        try:
            cur.execute(sql)
            # 查询结果条数
            # row = cur.rowcount
            # 查询结果集
            dataList = cur.fetchall()
        except Exception as e:
            print(e)

        # 关闭游标
        cur.close()

        # 关闭数据连接
        self.db.close()

        # 返回查询结果集
        return dataList

    # return dataList,row

    '''
    将基本信息持久化到MongoDB
    '''

    def Date_Sold_Persistence(self, sql):
        # 初始化数据库连接对象
        db1 = DbUtil(DB_CONFIG['dbHost'], DB_CONFIG['dbPort'], DB_CONFIG['dbUser'],
                     DB_CONFIG['dbPass'], '618')
        # 获取基本信息
        datalist = db1.queryOperation(sql)
        logging.info('下面是获取基本信息列表')
        logging.debug(datalist)

        # client = pymongo.MongoClient(host='127.0.0.1', port=27017)

        '''
        选择MnogoDB数据库test
        选择集合newdatalist
         '''
        db = client.test  # test数据库
        p = db.item_moco_tmall_sold  # datalist集合

        # 将datalist数据持久化到MongoDB中
        for rows in datalist:
            p.insert_one(rows)

        logging.info('基本信息持久化成功')


    '''
        每个商品的日销售量变化
    '''
    def date_sold_test(self,item_id_x):

        sql = "select item_id,rtime,totalSoldQuantity from `618`.item_73hours_tmall where item_id= {0} and sku=0".format(item_id_x)
        print(sql)
        # 初始化数据库连接对象
        db1 = DbUtil(DB_CONFIG['dbHost'], DB_CONFIG['dbPort'], DB_CONFIG['dbUser'],
                     DB_CONFIG['dbPass'], '618')
        # 获取基本信息
        datalist = db1.queryOperation(sql)
       # print(type(datalist))
        logging.info('下面是获取基本信息列表--------->每个商品的日销售量变化')
        logging.debug(datalist)

        # client = pymongo.MongoClient(host='127.0.0.1', port=27017)

        #print(datalist['rtime'].strtime('%Y'))


        temp_sold={}
        print(type(temp_sold))
        for index in range(0,len(datalist)-1):
           # today_sold = rows['totalSoldQuantity']
            #today_sold = int(datalist[x]['totalSoldQuantity'])
            #logging.info('gggg')
            #print(today_sold)
            if index == 0:
                pass
            else:
                today = int(datalist[index]['totalSoldQuantity']) - int(datalist[index-1]['totalSoldQuantity'])
                if today < 0:
                    today = 0
                ptime1 = datalist[index]['rtime']
                ptime2 = datalist[index - 1]['rtime']
                print(type(ptime1))
                day = self.daytest(ptime1, ptime2)
                logging.info(day)
                temp_sold['item_id'] = item_id_x
                if day > 1 :
                    p = 1
                    while p <= day:
                         ptime2 = ptime2 + datetime.timedelta(days=1)
                         time = ptime2.strftime('%Y-%m-%d')
                         temp_sold[time] = 0
                         p=p+1
                elif day==1:
                        time = ptime1.strftime('%Y-%m-%d')
                        temp_sold[time] = today
                else:
                    pass



                # time = time_get()
                # if time.Cleartime(datalist[x-1]['rtime']) == time.Cleartime(datalist[x]['rtime']):
                #
                # else:
                # time = time_get()
                # ptime =time.Cleartime(datalist[index-1]['rtime'])
                #
                # temp_sold['item_id'] = item_id_x
                #
                #
                # #以ptime作为key,today作为value
                # temp_sold[ptime] = today

                #temp_sold = dict(temp_sold,**a)
                # print(datalist[x]['rtime'])
                print(datalist[index]['item_id'])
                print(today)
                print(temp_sold)


        '''
        插入到MongoDB
        '''
        #print(datalist)
        db = client.test  # test数据库
        p = db.item_moco_tmall_sold  # datalist集合
        p.insert_one(temp_sold)
        # for rows in temp_sold:
                #print(datalist[x]['rtime'].strftime('%Y-%m-%d'))
                #print(ptime)


                #print(rows['totalSoldQuantity'])
                #print(today)
                #print(today)
        #print(today)
        #print(datalist)


    '''
    每个商品的每天价格变化
    '''
    def date_price_test(self,item_id_y):

        sql = "select item_id,rtime,price from `618`.item_73hours_tmall where item_id= {0} and sku=0".format(item_id_y)
        # 初始化数据库连接对象
        db1 = DbUtil(DB_CONFIG['dbHost'], DB_CONFIG['dbPort'], DB_CONFIG['dbUser'],
                     DB_CONFIG['dbPass'], '618')
        # 获取基本信息
        datalist = db1.queryOperation(sql)
        #logging.info('下面是获取基本信息列表------->每个商品每天的价格变化')
        #logging.debug(datalist)
        temp_price={}
        print(type(temp_price))
        for index in range(0,len(datalist)-1):
            if index == 0:
                pass
            else:
                # time = time_get()
                # ptime1 = time.Cleartime(datalist[index]['rtime'])
                # print(type(ptime1))
                # ptime2 = time.Cleartime(datalist[index-1]['rtime'])
                ptime1 = datalist[index]['rtime']
                ptime2 = datalist[index-1]['rtime']
                print(type(ptime1))
                day = self.daytest(ptime1,ptime2)
                logging.info(day)
                price = datalist[index]['price']
                temp_price['item_id'] = item_id_y
                if day > 1 :
                    p = 1
                    while p <= day:
                         ptime2 = ptime2 + datetime.timedelta(days=1)
                         time = ptime2.strftime('%Y-%m-%d')
                         temp_price[time] = price
                         p=p+1
                elif day==1:
                        time = ptime1.strftime('%Y-%m-%d')
                        temp_price[time] = price
                else:
                    pass
            # temp_price['item_id'] = item_id_y
            # temp_price[ptime1] = datalist[index]['price']

        db = client.test  # test数据库
        p = db.item_moco_tmall_price  # datalist集合
        # for rows in temp_sold:
        p.insert_one(temp_price)

    '''
    1、转换为str类型(为了使得转化成datetime.datetime类型的时分秒为00:00:00)
    2、转化成datetime.datetime类型
    3、后一天 - 前一天判断差的日子
    4、通过d.days得到整型数据差值
    '''
    def daytest(self,ptime_1,ptime_2):
        dtime1 = ptime_1.strftime('%Y-%m-%d')
        print(type(dtime1))
        d1 = datetime.datetime.strptime(dtime1, "%Y-%m-%d")

        dtime2 = ptime_2.strftime('%Y-%m-%d')
        d2 = datetime.datetime.strptime(dtime2, "%Y-%m-%d")
        print(type(d2))
        d = d1 - d2
        return d.days

    '''
     获取某店铺的所有item_id
    '''
    def GetSql_item_id(self, sql,flag):
        db1 = DbUtil(DB_CONFIG['dbHost'], DB_CONFIG['dbPort'], DB_CONFIG['dbUser'],
                     DB_CONFIG['dbPass'], '618')
        # 获取基本信息
        datalist = db1.queryOperation(sql)
        print(datalist)

        # flag==0则执行日销量变化
        # flag==1则执行日价格变化
        if flag==0:
            for rows in datalist:
                self.date_sold_test(rows['item_id'])
        else:
            for rows in datalist:
                self.date_price_test(rows['item_id'])









if __name__ == '__main__':
    db1 = DbUtil(DB_CONFIG['dbHost'], DB_CONFIG['dbPort'], DB_CONFIG['dbUser'],
                 DB_CONFIG['dbPass'], '618')

    '''
    信息持久化
    '''
   # sql = "select item_id,rtime,totalSoldQuantity from `618`.item_73hours_tmall where item_id={0} and sku=0"
    sql = "select item_id from `618`.item_73hours_tmall where sku=0 group by item_id"
    db1.GetSql_item_id(sql,0)
