# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json

from operate_db import operate_redis
import config

class WeatherBackPipeline(object):
    def __init__(self):
        self.redis_conn = operate_redis.get_redis_conn()

    def process_item(self, item, spider):
        if item['area'] == 'county':
            self.redis_conn.rpush(config.redis_list_name_config['COUNTY_URL'], json.dumps(dict(item)))
        elif item['area'] == 'township':
            self.redis_conn.rpush(config.redis_list_name_config['TOWNSHIP_URL'], json.dumps(dict(item)))
        return item
