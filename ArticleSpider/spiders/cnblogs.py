from urllib import parse
import re
import json

import scrapy
from scrapy import Request
import requests

from ArticleSpider.utils import common
from ArticleSpider.items import CnblogsArticleItem


class CnblogsSpider(scrapy.Spider):
    name = 'cnblogs'
    allowed_domains = ['news.cnblogs.com']
    start_urls = ['http://news.cnblogs.com/']

    def parse(self, response):
        """
        1. 获取新闻列表页中的新闻 url 并交给 scrapy 进行下载，调用相应的解析方法
        2. 获取下一页的 url 交给 scrapy 继续进行下载解析
        :param response:
        :return:
        """
        post_nodes = response.css('#news_list .news_block')[:1]
        for post_node in post_nodes:
            image_url = post_node.css('.entry_summary a img::attr(src)').extract_first('')
            if image_url.startswith("//"):
                image_url = "https:" + image_url
            post_url = post_node.css('h2 a::attr(href)').extract_first('')
            yield Request(
                url=parse.urljoin(response.url, post_url),
                meta={'front_image_url': image_url},
                callback=self.parse_detail
            )

        # 提取下一页并继续交给 scrapy engine 进行下载
        # css 选择器写法
        # next_url = response.css('div.pager a:last-child::text').extract_first('')
        # if next_url == "Next >":
        #     next_url = response.css('div.pager a:last-child::attr(href)').extract_first('')
        #     yield Request(url=parse.urljoin(response.url, next_url), callback=self.parse)

        # xpath 写法
        # next_url = response.xpath("//a[contains(text(), 'Next >')]/@href").extract_first('')
        # yield Request(url=parse.urljoin(response.url, next_url), callback=self.parse)

    def parse_detail(self, response):
        post_id_reg = re.match(".*?(\d+)", response.url)
        if post_id_reg:
            post_id = post_id_reg.group(1)

            article_item = CnblogsArticleItem()
            title = response.css('#news_title a::text').extract_first('')
            # title = response.xpath("//*[@id='news_title']//a/text()").extract_first('')
            create_date = response.css('#news_info .time::text').extract_first('')
            create_date_reg = re.match(".*?(\d+.*)", create_date)
            if create_date_reg:
                create_date = create_date_reg.group(1)
            # create_date = response.xpath("//*[@id='news_info']//*[@class='time']/text()").extract_first('')
            content = response.css('#news_content').extract()[0]
            # content = response.xpath("//*[@id='news_content']").extract()[0]
            tag_list = response.css('.news_tags a::text').extract()
            # tag_list = response.xpath("//*[@class='news_tags']//a/text()").extract()
            tags = '、'.join(tag_list)

            article_item['post_id'] = post_id
            article_item['url'] = response.url
            article_item['url_object_id'] = common.get_md5(response.url)
            article_item['title'] = title
            article_item['create_date'] = create_date
            article_item['content'] = content
            article_item['tags'] = tags
            # 图片保存 imagePipeline 默认会以数组进行处理，此处必须存储为数组结构
            if response.meta.get('front_image_url', ''):
                article_item['front_image_url'] = [response.meta.get('front_image_url', '')]
            else:
                article_item['front_image_url'] = []

            # requests.get 为同步方法，不推荐使用
            # html = requests.get()
            # j_data = json.loads(html.text)
            # total_view = j_data['TotalView']
            # dig_count = j_data['DiggCount']
            # bury_count = j_data['BuryCount']
            # comment_count = j_data['CommentCount']

            # 继续使用 yield 进行异步处理
            yield Request(url=parse.urljoin(
                response.url,
                '/NewsAjax/GetAjaxNewsInfo?contentId={}'.format(post_id)),
                callback=self.parse_nums,
                meta={'article_item': article_item}
            )

    def parse_nums(self, response):
        j_data = json.loads(response.text)
        total_view = j_data['TotalView']
        dig_count = j_data['DiggCount']
        bury_count = j_data['BuryCount']
        comment_count = j_data['CommentCount']

        article_item = response.meta.get('article_item', '')
        article_item['total_view'] = total_view
        article_item['dig_count'] = dig_count
        article_item['bury_count'] = bury_count
        article_item['comment_count'] = comment_count

        yield article_item

        pass
