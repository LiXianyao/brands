#-*-coding:utf8-*-#
import ConfigParser
import redis
import sys
sys.path.append("..")
from consoleLogger import logger

class RedisConnection:
    rank_key_prefix = "bRank::"
    data_key_prefix = "bData::"
    py_key_prefix = "bPySet::"  # set
    item_key_prefix = "bItem::"

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
        for class_no in range(1, 46):
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

    def count_brand_no(self):
        brand_no_sum = 0
        for class_no in range(1, 46):
            brand_cnt = self.db.get("bRank::" + str(class_no) + "::cnt")
            print "class %d has brand no size %s" % (class_no, brand_cnt)
            brand_no_sum += int(brand_cnt)
        print "brand no intotal with %d" % brand_no_sum

    ####从redis中读取读音组合对应的商标号集合
    ##大于等于二元组的，用redis去算交集
    def get_pycombi(self, combi, class_no):
        inter_args = []
        combi_len = len(combi)
        first_key = self.py_key_prefix + str(class_no) + "::" + combi[0]
        combi_str = combi[0]

        for i in range(1, combi_len):
            set_key = self.py_key_prefix + str(class_no) + "::" + combi[i]
            combi_str += "," + combi[i]
            inter_args.append(set_key)

        inter = self.db.sinter(first_key, *tuple(inter_args))
        return inter, combi_str

    ##根据编号集合和大类号，获取对应的数据
    def get_union_data(self, class_no, union):
        for bid in union:
            bdata_key = self.data_key_prefix + str(class_no) + "::" + str(bid)
            self.pipe.hgetall(bdata_key)
        return self.pipe.execute()

if __name__=="__main__":
    con = RedisConnection()
    #con.clear_redis_key_multi()
    con.count_brand_no()

