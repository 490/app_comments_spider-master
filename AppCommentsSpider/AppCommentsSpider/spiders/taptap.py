# -*- coding: utf-8 -*-
from scrapy_redis.spiders import RedisSpider
from items import TaptapspiderItem, TaptapspiderItemLoader
import scrapy


class TaptapSpider(RedisSpider):
    """
    有三个属性，name，allowed_domains，start_urls
    每个项目里name是唯一的，用来区分不同的Spider。
    allowed_domains允许爬取的域名，如果初始或后续的请求链接不是这个域名下的，就会被过滤掉。
    start_urls，包含了Spider在启动时爬取的url列表，初始请求是由它来定义的。
    """
    name = 'taptap'
    allowed_domains = ['http://www.taptap.com']
    redis_key = 'taptap:start_urls'

    """
    parse是Spider的一个方法，默认情况下，被调用时start_urls里面的链接构成的请求完成下载后，
    返回的response就会作为唯一的参数传递给这个函数，parse方法负责解析返回的response，
    提取数据或者进一步生成要处理的请求,可使用的选择器为css或xpath。
    """
    def parse(self, response):
        """
        获取评论的页面
        :param response:
        :return:
        """
        review_url = response.xpath('//div[@class="main-header-tab"]/ul/li[2]/a/@href').extract_first("")
        # print('review_url', review_url)
        if review_url:
            yield scrapy.Request(url=review_url, callback=self.parse_reviews, dont_filter=True)

    def parse_reviews(self, response):
        """
        抓取页面中的内容
        :param response:
        :return:
        next_url是获取下一个页面的链接
        """
        next_url = response.xpath('//div[@class="main-body-footer"]//ul/li[last()]/a/@href').extract_first("")
        selector = scrapy.Selector(response)
        reviews = selector.xpath('//*[contains(@class, "taptap-review-item")]')
        for review in reviews:
            """
            item相当于一个字典，存储着这些提取出来的东西
            """
            item_loader = TaptapspiderItemLoader(item=TaptapspiderItem(), selector=review)
            item_loader.add_value("url", response.url)
            item_loader.add_xpath("user_name", '@data-user')
            item_loader.add_xpath("comment", 'div/div[3]//text()')
            item_loader.add_xpath("comment_time", 'div/div[1]/a/span/span[2]/text()')
            item_loader.add_xpath("phone", 'div/div[4]/span/text()')
            item_loader.add_xpath("like_it", 'div/div[4]/ul/li[2]/button/span/text()')
            item_loader.add_xpath("dislike_it", 'div/div[4]/ul/li[3]/button/span/text()')
            item_loader.add_xpath("rate", 'div/div[2]/i[1]/@style')
            reviews_loader = item_loader.load_item()
            yield reviews_loader
        # print('next_url', next_url)
        if next_url:
            yield scrapy.Request(url=next_url, callback=self.parse_reviews, dont_filter=True)


