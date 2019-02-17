import config
from operate_db.operate_mongo import OperateMongodb
from operate_db import operate_redis

import datetime

# delete redis data
redis_conn = operate_redis.get_redis_conn()
redis_conn.delete(config.redis_list_name_config['COUNTY_URL'])
redis_conn.delete(config.redis_list_name_config['TOWNSHIP_URL'])

# delete mongo data
om = OperateMongodb()
mongo_db = om.get_mongodb_db()
collection_seven_days_weather = mongo_db.get_collection('seven_days_weather')
collection_tf_hours_weather = mongo_db.get_collection('tf_hours_weather')
collection_life_weather = mongo_db.get_collection('life_weather')
# now date
now_date_str = datetime.datetime.now().strftime('%Y-%m-%d')

collection_life_weather.delete_many({'date':now_date_str})
collection_seven_days_weather.delete_many({'date':now_date_str})
collection_tf_hours_weather.delete_many({'date':now_date_str})
# close mongo
om.close_mongodb()
