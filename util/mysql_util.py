#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Author  : linjie
# @Des     : mysql工具类

import logging

import pymysql

# mysql_conf = {
#     'host': 'cdb-60q89up0.cd.tencentcdb.com',
#     'user': 'root',
#     'password': 'eV7-az6-GrZ-UFQ',
#     'port': 10023,
#     'database': 'tm_comment',
#     'charset': 'utf8'
# }
mysql_conf = {
    'host': 'localhost',
    'user': 'root',
    'password': '1111',
    'port': 3306,
    'database': 'wcspider',
    'charset': 'utf8'
}

class MySQLUtil:
    def __init__(self, conf):
        logging.basicConfig(level=logging.DEBUG,
                            format='[%(asctime)s] %(levelname)s [%(funcName)s: %(filename)s, %(lineno)d] %(message)s',
                            datefmt='%Y-%m-%d %H:%M:%S')
        self.conn = pymysql.connect(**conf)
        self.cursor = self.conn.cursor()

    # 获取游标
    def get_cur(self):
        return self.cursor

    # 事务提交
    def commit(self):
        self.conn.commit()

    # 关闭连接
    def close(self):
        self.conn.close()

    #关闭游标
    def curclose(self):
        self.cursor.close()

    # 回滚事务
    def rollback(self):
        self.conn.rollback()

    '''
        查询操作
    '''
    def queryOperation(self, sql):
        # 获取数据库游标
        cur = self.get_cur()
        #print("sdasds")
        # 执行查询
        dataList = []
        try:
            cur.execute(sql)
        # 查询结果条数
        # row = cur.rowcount
        # 查询结果集
            dataList = cur.fetchall()
        except Exception as e:
            logging.error('查询结果集异常{0}'.format(e))
        # 关闭游标
        #cur.close()
        # 关闭数据连接
        #self.close()
        #返回查询结果集
        return dataList

    def insertOperation(self,sql):
        cur = self.get_cur()
        try:
            cur.execute(sql)
            self.commit()
        except Exception as e:
            #print(e)
            logging.error('插入失败 {}'.format(e))
            self.rollback()

        #cur.close()
        #self.close()

    '''
    更新操作
    '''
    def updateOperation(self, sql):
        cur = self.get_cur()
        try:
            cur.execute(sql)
            self.commit()
        except Exception as e:
            logging.error('更新操作异常{0}'.format(e))
            self.rollback()
        #cur.close()
        #self.close()

    '''
    创建相应店铺的表
    '''
    def createTable(self,tablename):
        cur = self.get_cur()
        #sql = "CREATE TABLE {0} (item_id BIGINT(1),comment text,comment_time date)".format(tablename)
        sql = "CREATE TABLE {0}(item_id BIGINT(1) NOT NULL ,title VARCHAR(255) " \
              ",sold BIGINT(1) ,totalSoldQuantity BIGINT(1) ,skuurl VARCHAR(255) " \
              ",price DECIMAL(10,2),imgurl VARCHAR(255) ," \
              "spider_start_time DATETIME ,spider_last_time DATETIME ," \
              "PRIMARY KEY (item_id));".format(tablename)
        try:
            cur.execute(sql)
            self.commit()
        except Exception as e:
            logging.error('创建表异常{0}'.format(e))
            self.rollback()
        #cur.close()
        #self.close()

    '''
    插入评论数据
    '''
    def insertdata(self,goodsname,id,comment,comment_time):
        try:
            sql = "insert into {0}(item_id,comment,comment_time) value('{1}','{2}','{3}')".format(goodsname,id,comment,comment_time)
            logging.info('{}'.format(sql))
            self.insertOperation(sql)
        except Exception as e:
            logging.error('插入数据异常 {}'.format(e))
        else:
            logging.info('插入成功')

    '''
    插入店铺信息
    '''
    def insertshopmsg(self,shop_id,shop_title,shop_url,shop_pin_title):
        try:
            sql = "insert into shopmsg(shop_id,shop_title,shop_url,shop_pin_title) value ('{0}','{1}','{2}','{3}')".format(shop_id,shop_title,shop_url,shop_pin_title)
            logging.info('{}'.format(sql))
            self.insertOperation(sql)
        except Exception as e:
            logging.error('插入数据到shop表异常 {}'.format(e))
        else:
            logging.info('插入数据到shop表成功')
    '''
    获取店铺拼音名称
    '''
    def getshopname(self,shop_url):
        sql = "SELECT shop_pin_title FROM wcspider.shopmsg where shop_url={0}".format(shop_url)
        try:
            logging.info('{}'.format(sql))
            self.queryOperation(sql)
        except Exception as e:
            logging.error('获取店铺拼音名称出现异常:{}'.format(e))
        else:
            logging.info('获取店铺拼音名称成功')

    '''
    插入店铺的商品信息
    '''
    def insertgoodsmsg(self,shopname,item_id,title,sold,totalSoldQuantity,skuurl,price,imgurl,spider_start_time,spider_last_time):
        sql = "insert into {0} (item_id,title,sold,totalSoldQuantity,skuurl,price,imgurl,spider_start_time,spider_last_time) value ('{1}','{2}','{3}','{4}','{5}','{6}','{7}','{8}','{9}')".format(shopname,item_id,title,sold,totalSoldQuantity,skuurl,price,imgurl,spider_start_time,spider_last_time)
        try:
            logging.info('{}'.format(sql))
            self.insertOperation(sql)
        except Exception as e:
            logging.error('插入数据到{0}表异常:{1}'.format(shopname,e))
        else:
            logging.info('插入数据到{}表成功'.format(shopname))
    # #获取所有总流程数据
    # #@staticmethod
    # def get_datalist(self,state):
    #     sql = "select t.id tid,ta.id taskid,t.sku_id skuid,u.formula_id formulaid,u.datasource_id datasourceid,u.collection_name collectionname,u.document_id documentid,ta.state state,t.data_time datatime from" \
    #           " sku u left join task t on t.sku_id=u.id left join task_allocation ta on ta.task_id=t.id where ta.state='{0}' limit 1".format(state)
    #     datalist = self.queryOperation(sql)
    #     return datalist
    #
    # #更新documentid的值
    # def updatedocumentid(self,data,skuid):
    #     try:
    #         update_documentid_sql = "UPDATE sku SET document_id = '{0}' where id ='{1}'".format(data,skuid)
    #         self.updateOperation(update_documentid_sql)
    #     except Exception as e:
    #         logging.error('更新documentid异常 {}'.format(e))
    #     else:
    #         logging.info('更新documentid成功')
    #
    # #更新任务状态
    # def updatestate(self,state,taskid):
    #     try:
    #         update_state_sql = "UPDATE task_allocation SET state = {0} where id ='{1}'".format(state,taskid)
    #         self.updateOperation(update_state_sql)
    #     except Exception as e:
    #         logging.error('更新任务状态异常 {}'.format(e))
    #     else:
    #         logging.info('更新状态成功')


if __name__ == '__main__':
    m = MySQLUtil(mysql_conf)
    # a = {"a": '1', "b": '2'}
    # b = {"b": '1', "c": '3'}
    # m.update("sss", a, b)
    # sets = {"a": 1, "b": 2}
    # conditions = [{"$or": [{"c": 3}, {"d": 4}]}]
    # sql = m.creat_update_sql("aa", sets, conditions)
    # print(sql)
    # sets = {"a": 1, "b": 2}
    # conditions = [{"aaa": 11}, {"$like%": {"c": 3}}]
    # sql = m.creat_update_sql("aa", sets, conditions)
    # print(sql)

