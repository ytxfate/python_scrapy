try:
    import xml.etree.cElementTree as ET
except ImportError:
    import xml.etree.ElementTree as ET

import requests
import pymongo
from pprint import pprint
import json

warning_url = 'http://www.weather.com.cn/data/alarm_xml/alarminfo.xml'
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/71.0.3578.98 Safari/537.36'}
# with open('alarminfo.xml', 'r', encoding='utf-8') as f:
#     xml_str = f.read()
# ========================================================== #
# 每个element对象都具有以下属性：
# 1. tag：string对象，表示数据代表的种类。
# 2. attrib：dictionary对象，表示附有的属性。
# 3. text：string对象，表示element的内容。
# 4. tail：string对象，表示element闭合之后的尾迹。
# <tag attrib1=1>text</tag>tail
# ========================================================== #


def main_func():
    resp = requests.get(warning_url, headers=headers)
    resp.encoding = 'utf-8'
    xml_str = resp.text

    dict_tmp = {}
    root = ET.fromstring(xml_str.replace('&#10;', ''))
    dict_tmp[root.tag] = root.attrib
    if 'dt' in dict_tmp[root.tag]:
        dict_tmp[root.tag]['dt'] = dict_tmp[root.tag]['dt'][:8]

    for father in root:
        dict_tmp[root.tag][father.tag] = {'Station': []}
        for child in father:
            dict_tmp[root.tag][father.tag]['Station'].append(child.attrib)

    # pprint(dict_tmp)
    # json.dump(dict_tmp, open('aa.json', 'w'), ensure_ascii=True, indent=2)

    conn = pymongo.MongoClient(host='127.0.0.1:27017')
    db = conn.get_database('weather')
    db.authenticate('user', 'mongodb@password')
    db.get_collection('warning_weather').insert(dict_tmp)
    conn.close()

# 定时执行
from apscheduler.schedulers.blocking import BlockingScheduler
sched = BlockingScheduler()
sched.add_job(main_func, 'cron', hour=8, minute = 1,second = 1)
sched.start()