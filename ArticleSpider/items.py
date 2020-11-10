# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ArticlespiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class CnblogsArticleItem(scrapy.Item):
    post_id = scrapy.Field()
    url = scrapy.Field()
    url_object_id = scrapy.Field()
    title = scrapy.Field()
    create_date = scrapy.Field()
    front_image_url = scrapy.Field()
    front_image_path = scrapy.Field()
    content = scrapy.Field()
    tags = scrapy.Field()
    total_view = scrapy.Field()
    dig_count = scrapy.Field()
    bury_count = scrapy.Field()
    comment_count = scrapy.Field()
