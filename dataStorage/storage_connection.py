#-*-coding:utf8-*-#
import ConfigParser
import redis
import sys
sys.path.append("..")
from consoleLogger import logger


class RedisConnection:
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
        logger.info(u"已建立数据库连接，Host=%s:%s,db=%s" % (self.redis_ip, self.redis_port, self.redis_db))
        self.pipe = self.db.pipeline()

    def clear_redis_key(self, prefix):
        old_data = self.db.keys(prefix + "*")
        for key in old_data:
            self.pipe.delete(key)
        self.pipe.execute()
        logger.info("delete keys of prefix '%s' with num: %d" % (prefix, len(old_data)))
        del old_data[:]

    def clear_redis_key_multi(self):
        for class_no in range(1,2):
            old_data = self.db.keys("bPySet::" + str(class_no) + "::*,*")
            print "class %d has pyset size %d"%(class_no, len(old_data))

            for key in old_data:
                if len(key.split(",")) > 1:
                    self.pipe.delete(key)
            self.pipe.execute()
            del old_data[:]
            old_data = self.db.keys("bPySet::" + str(class_no) + "::*,*")
            print "after clean, class %d has pyset size %d" % (class_no, len(old_data))
            print old_data
            del old_data[:]


if __name__=="__main__":
    con = RedisConnection()
    con.clear_redis_key_multi()

