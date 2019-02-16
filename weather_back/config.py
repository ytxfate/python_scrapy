# 配置信息

# mongodb
mongodb_config = {
    "HOST": "127.0.0.1",
    "PORT": 27017,
    "DB": "weather",
    "USERNAME": "user",
    "PASSWORD": "mongodb@password",
    "auth": True
}
# redis
redis_config = {
    'HOST': '127.0.0.1',
    'PORT': 6379
}
# redis list name
redis_list_name_config = {
    'IPPROXY': 'IPPROXY_list',
    'COUNTY_URL': 'COUNTY_URL_list',
    'TOWNSHIP_URL': 'TOWNSHIP_URL_list'
}