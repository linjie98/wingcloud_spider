#!/usr/bin/env python
# -*- coding: utf-8 -*-
# @Time    : 2018/9/17 下午8:53
# @Author  : linjie
# @File    : mysql_util.py
# @Des     : mysql工具类

import logging

import pymysql

mysql_conf = {
    'host': 'cdb-60q89up0.cd.tencentcdb.com',
    'user': 'root',
    'password': 'eV7-az6-GrZ-UFQ',
    'port': 10023,
    'database': 'tm_comment',
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
        cur.close()
        # 关闭数据连接
        self.close()
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

        cur.close()
        self.close()

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
        cur.close()
        self.close()

    '''
    创建相应店铺的表
    '''
    def createTable(self,tablename):
        cur = self.get_cur()
        sql = "CREATE TABLE {0} (item_id BIGINT(50),comment text,comment_time date)".format(tablename)
        try:
            cur.execute(sql)
            self.commit()
        except Exception as e:
            logging.error('创建表异常{0}'.format(e))
            self.rollback()
        cur.close()
        self.close()

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

