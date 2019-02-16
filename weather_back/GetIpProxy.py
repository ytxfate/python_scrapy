import logging
import socket
import requests
import time
from bs4 import BeautifulSoup
import json

from operate_db import operate_redis
import config

class GetIpProxy:
    """
    爬取 ip 代理
    """
    USER_AGENT = 'Mozilla/5.0 (Windows NT 6.1; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36'
    HEADERS = {'User-Agent': USER_AGENT}
    def __init__(self):
        # 日志
        # logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', filename=log_file_name, filemode='a')
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        # redis 数据库
        self.redis_conn = operate_redis.get_redis_conn()
    
    def __get_proxy_page_html(self, name, url):
        """
        获取网页 HTML 代码
        """
        web_sign = True
        web_html = ''
        while web_sign:
            try:
                resp = requests.get(url, headers=self.HEADERS, timeout=5)
                if resp.status_code is 200:
                    self.logger.info("get url (" + name + ") web html data ending.")
                    web_html = resp.text
                    web_sign = False
                elif resp.status_code is 503:
                    self.logger.error("get url (" + name + ") web html error,restart after 60 second...")
                    time.sleep(63)
                else:
                    self.logger.error("get url (" + name + ") web html error,restart after 10 second...")
                    time.sleep(5)
            except Exception as e:
                self.logger.error(e)
                time.sleep(5)
        return web_html

    def get_ip_proxy(self):
        """
        通过爬虫获取 IP 代理
        """
        ip_proxy_url = "http://www.xicidaili.com/"
        proxy_html = self.__get_proxy_page_html("IP 代理", ip_proxy_url)
        bs_obj = BeautifulSoup(proxy_html, 'lxml')
        table_obj = bs_obj.find("table", {"id":"ip_list"})
        tr_objs = table_obj.findAll("tr")
        i = 0   # 记录 tr 标签中 th 出现的次数
        for tr_obj in tr_objs:
            if tr_obj.find("th"):
                i += 1
                if i == 3:  # i=3时,高匿IP代理爬取完毕
                    break
            td_objs = tr_obj.findAll("td")
            if len(td_objs) == 0:
                continue
            else:
                ip_str = td_objs[5].get_text().lower() + "://" + td_objs[1].get_text() + ":" + td_objs[2].get_text()
                dict_tmp = {
                    td_objs[5].get_text().lower(): ip_str
                }
            self.redis_conn.rpush(config.redis_list_name_config['IPPROXY'], json.dumps(dict_tmp))
        self.logger.info('get ip proxy ending.')
