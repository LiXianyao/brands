#-*-coding:utf8-*-#
import ConfigParser
import redis

class redis_connection:
    def __init__(self, config_file="storage_redis.config"):
        self.config_file_name = config_file
        ###读取配置文件
        self.cf = ConfigParser.ConfigParser()
        self.cf.read(self.config_file_name)
        self.redis_ip = self.cf.get("redis", "redis_ip")
        self.redis_port = self.cf.get("redis", "redis_port")
        self.redis_db = self.cf.get("redis", "redis_db")
        self.redis_pwd = self.cf.get("redis", "redis_pwd")
        ##创建redis连接及管道
        self.db = redis.StrictRedis(host=self.redis_ip, port=self.redis_port, db=self.redis_db, password=self.redis_pwd)
        self.pipe = self.db.pipeline()

    def clear_redis_key(self, prefix):
        old_data = self.db.keys(prefix + "*")
        for key in old_data:
            self.pipe.delete(key)
        self.pipe.execute()
        print "delete keys of prefix '%s' num: %d" % (prefix, len(old_data))
        del old_data[:]

