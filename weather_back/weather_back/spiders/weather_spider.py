import scrapy
from scrapy import log
import re
import json
import datetime

from weather_back.items import WeatherBackItem
from operate_db import operate_redis
from operate_db.operate_mongo import OperateMongodb
import config

# now date
now_date_str = datetime.datetime.now().strftime('%Y-%m-%d')

class WeatherSpider(scrapy.Spider):
    name = "weather_root"
    allowed_domains = ["weather.com.cn"]
    start_urls = ["http://www.weather.com.cn/weather1dn/101271601.shtml"]
    def __init__(self):
        self.om = OperateMongodb()
        mongo_db = self.om.get_mongodb_db()
        self.collection_seven_days_weather = mongo_db.get_collection('seven_days_weather')
        self.collection_tf_hours_weather = mongo_db.get_collection('tf_hours_weather')
        self.collection_life_weather = mongo_db.get_collection('life_weather')
        
    def parse(self, response):
        """
        提取县级url
        """
        if response.status == 200:
            # 删除所有县级地区 url 老数据
            redis_conn = operate_redis.get_redis_conn()
            redis_conn.delete(config.redis_list_name_config['COUNTY_URL'])
            # 提取数据
            div_containera_bss = response.xpath('//div[@class="containera"]')
            div_containera_bs = div_containera_bss[len(div_containera_bss) - 1]
            for a_tag in div_containera_bs.xpath('a'):
                item = WeatherBackItem()
                a_href = a_tag.xpath('@href').extract_first()
                a_text = a_tag.xpath('text()').extract_first()
                if a_text == '城区':
                    continue
                item['name'] = a_text
                item['href'] = a_href
                item['area'] = "county"
                yield item
                # 请求新的连接
                yield scrapy.Request(a_href, callback=self.parse_county)
            log.msg('get all county area urls ending.')
        else:
            yield scrapy.Request(response.url, callback=self.parse)
    
    def fromat_str_to_json(self, json_str, regexp_str):
        """
        正则提取数据，并装换为 json 格式
        """
        reg_exp = re.compile(regexp_str)
        groups = reg_exp.match(json_str)
        end_str = groups.group(1)
        end_json = json.loads(end_str)
        return end_json

    def format_seven_weather_info(self, list_tmp):
        """
        替换7天天气数据的 key
        """
        # 气温jb、风向jd(无风:0,东北:1,东:2,东南:3,南:4,西南:5,西:6,西北:7,北:8)、天气代码ja、时间jf、风力jc（0:<3,1:3~4,2:4~5...）
        dict_keys = {
            'ja':'weather_code',
            'jb':'temperature',
            'jc':'wind_power',
            'jd':'wind_direction',
            'jf':'date_hour'
        }
        dict_keys_wind = {
            "0":"无风",
            "1":"东北",
            "2":"东",
            "3":"东南",
            "4":"南",
            "5":"西南",
            "6":"西",
            "7":"西北",
            "8":"北"
        }
        list_end = []
        for list_t in list_tmp:
            list_e = []
            for dict_tmp in list_t:
                dict_end = {}
                for key, value in dict_tmp.items():
                    if key == 'je':
                        continue
                    dict_end[dict_keys[key]] = value
                    if key == 'jd':
                        dict_end[dict_keys[key]] = dict_keys_wind[value]
                list_e.append(dict_end)
            list_end.append(list_e)
        return list_end

    def parse_county_seven(self, response, title):
        seven_day = response.xpath('//div[@class="todayRight"]/script/text()').extract_first()
        # 字符串中包含有大量的 换行符 及 制表符 ,用 多行匹配 或替 换换行符后匹配
        # r'.*hour3data=(\[.*?\]);.*' 加 ? ，使正则匹配不进行 "贪婪" 匹配
        seven_day = self.fromat_str_to_json(str(seven_day).replace('\r\n', '').replace('\t', '').replace('\n', ''), r'.*hour3data=(\[.*?\]);.*')
        format_weather_info = []
        if seven_day:
            format_weather_info = self.format_seven_weather_info(seven_day)
        dict_insert_mongo = {
            "date": now_date_str,
            "area": 'county',
            "name": title,
            "weather_info": format_weather_info
        }
        log.msg('seven weather ' + title)
        self.collection_seven_days_weather.insert(dict_insert_mongo)

    def format_hours_weather_info(self, dict_tmp):
        """
        替换24小时图表数据的 key
        """
        # 时间(hour)，气温(temperature)，风向(wind_direction)，风力(wind_power)，降水(rain)，相对湿度(humidity)
        dict_keys = {
            "od21": "hour",
            "od22": "temperature",
            "od24": "wind_direction",
            "od25": "wind_power",
            "od26": "rain",
            "od27": "humidity"
        }
        hours_info = []
        if 'od' in dict_tmp:
            dict_tmp_od = dict_tmp['od']
            if 'od2' in dict_tmp_od:
                list_tmp = dict_tmp_od['od2']
                for dict_t in list_tmp:
                    dict_end = {}
                    for key, value in dict_t.items():
                        if key in dict_keys:
                            dict_end[dict_keys[key]] = value
                    hours_info.append(dict_end)
        return hours_info

    def parse_county_tables(self, response, title, area):
        """
        提取县级地区24小时图表信息
        """
        hours_data = response.xpath('//div[@class="weather_zdsk"]/script/text()').extract_first()
        # 字符串中包含有大量的 换行符 及 制表符 ,用 多行匹配 或替 换换行符后匹配
        hours = self.fromat_str_to_json(str(hours_data).replace('\r\n', '').replace('\t', '').replace('\n', ''), r'.*observe24h_data = (\{.*?\});.*')
        format_weather_info = []
        if hours:
            format_weather_info = self.format_hours_weather_info(hours)
        dict_insert_mongo = {}
        if area == 'county':
            dict_insert_mongo = {
                "date": now_date_str,
                "area": area,
                "name": title,
                "hours_info": format_weather_info
            }
        else:
            dict_insert_mongo = {
                "date": now_date_str,
                "area": area,
                "name": title[2:],
                'county':title[:2],
                "hours_info": format_weather_info
            }
        if dict_insert_mongo:
            self.collection_tf_hours_weather.insert(dict_insert_mongo)

    def parse_county_life(self, response, title, area):
        """
        提取生活助手
        """
        div_info = response.xpath('//div[@class="weather_shzs weather_shzs_1d"]')
        headers_end = []
        headers = div_info.xpath('//ul/li/h2/text()').extract()
        headers_end = []
        for header in headers:
            if '血糖' in header:
                header = '健臻·血糖'
            # reg_str = '<.*?>'
            # re_com = re.compile(reg_str)
            # header = re.sub(re_com, '', str(header))
            headers_end.append(header)
            
        ems_end = div_info.xpath('//div/dl/dt/em/text()').extract()
        
        dds_end = div_info.xpath('//div[@class="lv"]/dl/dd/text()').extract()

        # log.msg('===========================')
        # log.msg(headers_end)
        # log.msg(ems_end)
        # log.msg(dds_end)
        # 组合数据
        list_end = []
        for i, key in enumerate(headers_end):
            dict_t = {
                'a': key,
                'b': ems_end[i],
                'c': dds_end[i]
            }
            list_end.append(dict_t)
        if list_end:
            dict_insert_mongo = {}
            if area == 'county':
                dict_insert_mongo = {
                    "date": now_date_str,
                    "area": area,
                    "name": title,
                    "life_info": list_end
                }
            else:
                dict_insert_mongo = {
                    "date": now_date_str,
                    "area": area,
                    "name": title[2:],
                    'county':title[:2],
                    "life_info": list_end
                }
            # log.msg(dict_insert_mongo)
            self.collection_life_weather.insert(dict_insert_mongo)

    def parse_county(self, response):
        """
        提取乡级url 及县级天气信息
        """
        TOWNSHIP_URL_ONE = 'http://forecast.weather.com.cn/town/weather1dn/'
        TOWNSHIP_URL_SEVEN = 'http://forecast.weather.com.cn/town/weathern/'
        # if response.status == 200:
        #     # 删除所有乡级地区 url 老数据
        #     redis_conn = operate_redis.get_redis_conn()
        #     redis_conn.delete(config.redis_list_name_config['TOWNSHIP_URL'])
        #     # 删除当天爬取失败的数据
        #     self.collection_life_weather.delete_many({'date':now_date_str})
        #     self.collection_seven_days_weather.delete_many({'date':now_date_str})
        #     self.collection_tf_hours_weather.delete_many({'date':now_date_str})
        if response.status == 200:
            # ===== 提取乡级 url ===== #
            title = response.xpath('/html/head/title/text()').extract_first()
            reg_title = re.compile(r'.*【(.*?)天气】.*')
            return_title = reg_title.search(title.replace(' ', ''))
            web_title = return_title.group(1)
            # log.msg(web_title)
            div_script_str = response.xpath('//div[@id="weatherScrollContainer"]/script/text()').extract_first()
            reg_urls = re.compile(r'.*var villages = (\[.*?\]).*')
            return_urls = reg_urls.search(div_script_str)
            web_urls = return_urls.group(1)
            url_infos = json.loads(web_urls)
            for url_info in url_infos:
                item = WeatherBackItem()
                item["name"] = url_info['name']
                item["href_code"] = url_info['id']
                item["area"] = 'township'
                item["county"] = web_title
                yield item
                # 请求新的连接
                # 七天
                yield scrapy.Request(TOWNSHIP_URL_SEVEN + url_info['id'] + '.shtml', callback=self.parse_township_seven)
                # 图表、生活助手
                yield scrapy.Request(TOWNSHIP_URL_ONE + url_info['id'] + '.shtml', callback=self.parse_township_table_life)
            
            # ===== 提取天气数据 ===== #
            # 七天
            self.parse_county_seven(response, web_title)
            # 图表
            self.parse_county_tables(response, web_title, 'county')
            # 生活助手
            self.parse_county_life(response, web_title, 'county')
        else:
            yield scrapy.Request(response.url, callback=self.parse_county)

    def parse_township_seven(self, response):
        """
        提取乡级七天天气数据
        """
        if response.status == 200:
            title = response.xpath('/html/head/title/text()').extract_first()
            reg_title = re.compile(r'.*【(.*?)天气】.*')
            return_title = reg_title.search(title.replace(' ', ''))
            web_title = return_title.group(1)
            div_seven_day = response.xpath('//div[@class="blueFor-container"]/script/text()').extract_first()
            seven_day = self.fromat_str_to_json(str(div_seven_day).replace('\r\n', '').replace('\t', '').replace('\n', ''), r'.*hour3data = (\[.*?\]);.*')
            # 格式化
            format_weather_info = []
            if seven_day:
                format_weather_info = self.format_seven_weather_info(seven_day)
            dict_insert_mongo = {
                "date": now_date_str,
                "area": 'township',
                "name": web_title[2:],
                'county':web_title[:2],
                "weather_info": format_weather_info
            }
            log.msg('seven weather ' + title)
            self.collection_seven_days_weather.insert(dict_insert_mongo)
        else:
            yield scrapy.Request(response.url, callback=self.parse_township_seven)

    def parse_township_table_life(self, response):
        """
        提取乡级24小时及生活助手数据
        """
        if response.status == 200:
            title = response.xpath('/html/head/title/text()').extract_first()
            reg_title = re.compile(r'.*【(.*?)天气】.*')
            return_title = reg_title.search(title.replace(' ', ''))
            web_title = return_title.group(1)
            # 图表
            self.parse_county_tables(response, web_title, 'township')
            # 生活助手
            self.parse_county_life(response, web_title, 'township')
        else:
            yield scrapy.Request(response.url, callback=self.parse_township_table_life)
