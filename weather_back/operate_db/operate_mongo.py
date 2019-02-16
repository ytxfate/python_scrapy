from pymongo import MongoClient

import config

class OperateMongodb:
    """
    操作 mongodb 数据库
    """
    def __init__(self):
        """
        连接 mongodb 数据库
        """
        # mongodb 配置信息
        self.mongodb_config = config.mongodb_config
        self.conn = MongoClient(self.mongodb_config['HOST'], self.mongodb_config['PORT'])

    def get_mongodb_db(self):
        """
        连接操作数据库
        """
        db = self.conn[self.mongodb_config['DB']]
        # 判断是否进行用户验证
        if self.mongodb_config['auth']:
            db.authenticate(name=self.mongodb_config['USERNAME'], password=self.mongodb_config['PASSWORD'])
        return db

    def close_mongodb(self):
        """
        关闭数据库连接
        """
        self.conn.close()
