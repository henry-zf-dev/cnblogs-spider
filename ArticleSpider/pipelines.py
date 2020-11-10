# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter

import codecs
import json

from scrapy.pipelines.images import ImagesPipeline
from scrapy.exporters import JsonItemExporter
import MySQLdb
from twisted.enterprise import adbapi


class ArticlespiderPipeline:
    def process_item(self, item, spider):
        return item


# 写入 MySQL 数据库
class MysqlPipeline(object):
    def __init__(self):
        self.conn = MySQLdb.connect('127.0.0.1', 'root', '123456', 'article_spider', charset='utf8', use_unicode=True)
        self.cursor = self.conn.cursor()

    def process_item(self, item, spider):
        insert_sql = """
            insert into cnblogs_article(post_id, create_date, title, url, url_object_id, front_image_url, front_image_path, content, tags, total_view, dig_count, bury_count, comment_count)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE total_view=VALUES(total_view), dig_count=VALUES(dig_count), bury_count=VALUES(bury_count), comment_count=VALUES(comment_count)
        """
        params = list()
        params.append(item.get('post_id', ''))
        params.append(item.get('create_date', '1970-07-01'))
        params.append(item.get('title', ''))
        params.append(item.get('url', ''))
        params.append(item.get('url_object_id', ''))
        params.append(','.join(item.get('front_image_url', [])))
        params.append(item.get('front_image_path', ''))
        params.append(item.get('content', ''))
        params.append(item.get('tags', ''))
        params.append(item.get('total_view', 0))
        params.append(item.get('dig_count', 0))
        params.append(item.get('bury_count', 0))
        params.append(item.get('comment_count', 0))
        self.cursor.execute(insert_sql, tuple(params))
        self.conn.commit()
        return item


class MysqlTwistedPipeline(object):
    def __init__(self, db_pool):
        self.db_pool = db_pool

    @classmethod
    def from_settings(cls, settings):
        from MySQLdb.cursors import DictCursor
        db_params = dict(
            host=settings['MYSQL_HOST'],
            db=settings['MYSQL_DBNAME'],
            user=settings['MYSQL_USER'],
            password=settings['MYSQL_PASSWORD'],
            charset='utf8',
            cursorclass=DictCursor,
            use_unicode=True
        )
        db_pool = adbapi.ConnectionPool('MySQLdb', **db_params)
        return cls(db_pool)

    def process_item(self, item, spider):
        query = self.db_pool.runInteraction(self.do_insert, item)
        query.addErrback(self.handle_error)

    def handle_error(self, failure, item, spider):
        print(failure)

    def do_insert(self, cursor, item):
        insert_sql = """
            insert into cnblogs_article(post_id, create_date, title, url, url_object_id, front_image_url, front_image_path, content, tags, total_view, dig_count, bury_count, comment_count)
            values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ON DUPLICATE KEY UPDATE total_view=VALUES(total_view), dig_count=VALUES(dig_count), bury_count=VALUES(bury_count), comment_count=VALUES(comment_count)
        """
        params = list()
        params.append(item.get('post_id', ''))
        params.append(item.get('create_date', '1970-07-01'))
        params.append(item.get('title', ''))
        params.append(item.get('url', ''))
        params.append(item.get('url_object_id', ''))
        params.append(','.join(item.get('front_image_url', [])))
        params.append(item.get('front_image_path', ''))
        params.append(item.get('content', ''))
        params.append(item.get('tags', ''))
        params.append(item.get('total_view', 0))
        params.append(item.get('dig_count', 0))
        params.append(item.get('bury_count', 0))
        params.append(item.get('comment_count', 0))

        cursor.execute(insert_sql, tuple(params))


# 自定义 JSON 文件的导出
class JsonWithEncodingPipeline(object):
    def __init__(self):
        # w : 每次覆盖文件
        # a : 追加文件信息
        self.file = codecs.open('article.json', 'a', encoding='utf-8')

    def process_item(self, item, spider):
        lines = json.dumps(dict(item), ensure_ascii=False) + '\n'
        self.file.write(lines)
        return item

    def spider_closed(self, spider):
        self.file.close()


# 内置 JSON 文件的导出
class JsonExporterPipeline(object):
    def __init__(self):
        # w : 每次覆盖文件
        # a : 追加文件信息
        # b : 以二进制保存
        self.file = open('articleExport.json', 'wb')
        self.exporter = JsonItemExporter(self.file, encoding='utf-8', ensure_ascii=False)
        self.exporter.start_exporting()

    def process_item(self, item, spider):
        self.exporter.export_item(item)
        return item

    def spider_closed(self, spider):
        self.exporter.finish_exporting()
        self.file.close()


class ArticleImagePipeline(ImagesPipeline):
    # 重写 ImagesPipeline 类的 item_completed 方法，自定义图片下载拦截
    def item_completed(self, results, item, info):
        if 'front_image_url' in item:
            for ok, value in results:
                image_file_path = value['path']
            item['front_image_path'] = image_file_path

        return item
