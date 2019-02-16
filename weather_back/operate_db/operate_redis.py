import redis

import config

def get_redis_conn():
    """
    获取 redis 连接
    """
    redis_conn = redis.Redis(host=config.redis_config['HOST'], port=config.redis_config['PORT'])
    return redis_conn