# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import pymysql
import logging
import pymysql.cursors
from twisted.enterprise import adbapi
logger = logging.getLogger(__name__)


class AppcommentsspiderPipeline(object):
    def __init__(self, dbpool):
        self.dbpool = dbpool

    @classmethod
    def from_settings(cls, settings):
        """
        载入数据库的配置
        :param settings:
        :return:
        """
        dbparms = dict(
            host=settings["MYSQL_HOST"],
            db=settings["MYSQL_DBNAME"],
            user=settings["MYSQL_USER"],
            passwd=settings["MYSQL_PASSWORD"],
            charset='utf-8',
            cursorclass=pymysql.cursors.DictCursor,
            use_unicode=True,
        )
        dbpool = adbapi.ConnectionPool("MySQLdb", **dbparms)
        return cls(dbpool)

    """
    Item Pipeline意为项目管道，当生成Item后，它会自动被送到Item Pipeline进行处理，
    常用它来做的操作：清理HTML数据;验证爬取数据，检查爬取字段;查重并丢弃重复内容;将爬取结果储存到数据库。
    要实现一个Item Pipeline，只需要定义一个类并实现process_item方法即可，
    启用后，Item Pipeline会自动调用这个方法，这个方法必须返回包含数据的字典或是Item对象，或者抛出DropItem异常。
    """
    def process_item(self, item, spider):
        """
        使用twisted将mysql插入变成异步执行
        :param item:
        :param spider:
        :return:
        """
        for field in item.fields:
            item.setdefault(field, '')
        query = self.dbpool.runInteraction(self.do_insert, item)
        # 处理异常
        query.addErrback(self.handle_error, item, spider)

    def handle_error(self, failure, item, spider):
        # log中写入异步插入的异常
        logging.error(failure)

    def do_insert(self, cursor, item):
        """
        执行具体的插入
        根据不同的item构建不同的sql语句并插入到mysql中
        :param cursor:
        :param item:
        :return:
        """
        insert_sql, params = item.get_insert_sql()
        cursor.execute(insert_sql, params)

