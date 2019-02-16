# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class WeatherBackItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    name = scrapy.Field()
    href = scrapy.Field()
    area = scrapy.Field()
    county = scrapy.Field()
    href_code = scrapy.Field()

class IpProxyBackItem(scrapy.Item):
    ip_proxy = scrapy.Field()