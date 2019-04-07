#-*-coding:utf8-*-#
from brand_item import BrandItem
import time
import json
import traceback
from storage_connection import RedisConnection
import pandas as pd
import sys
from pypinyin import lazy_pinyin, Style
from itertools import combinations
import os
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append('..')
from brandInfo.csvReader import CsvReader
from consoleLogger import logger
from similarity import brand

def load_brand_item():
    item_list = BrandItem.query.all()
    item_dict = {}
    for item in item_list:
        group_no = int(item.group_no)
        item_name = item.item_name
        item_no = item.item_no
        class_no = int(item.class_no)
        if group_no not in item_dict:
            item_dict[group_no] = {"class_no": class_no}
        item_dict[group_no][item_name] = item_no
    #for item in item_dict[101]:
    #    print item, item_dict[101][item]
    #print item_dict.keys()
    return item_dict

###解析csv数据行中的小项列表json字符串
def analysis_product_list(line):
    product_list_head = len(line) - 5
    product_list = line[product_list_head].replace("]\"", "]").replace("\"[", "[").replace("\"\"", "\"")
    flag = True
    product_list_array = []
    while flag and product_list_head > 3:
        try:
            product_list_array = json.loads(product_list)
            flag = False
        except:
            product_list_head -= 1
            product_list = line[product_list_head].replace("]\"", "]").replace("\"[", "[").replace("\"\"", "\"") \
                           + "," + product_list
    return flag, product_list_head, product_list_array

class DataStorage:
    date_origin_format = "%Y-%m-%d %H:%M:%S"
    date_target_format = "%Y%m%d"

    rank_key_prefix = "bRank::"
    data_key_prefix = "bData::"
    py_key_prefix = "bPySet::"  # set
    item_key_prefix = "bItem::"

    info_dict = "../brandInfo/"
    info_csv_name = u"注册商标基本信息.csv"
    item_csv_name = u"注册商标商品服务信息.csv"

    def __init__(self, clean_out=False):
        self.redis_con = RedisConnection()
        self.csv_reader = CsvReader()
        if clean_out:
            logger.info(u"数据库重置开启，开始清洗数据库")
            self.reset_redis_data()
            logger.info(u"数据库清洗完毕")
        #读取要转储的压缩包文件名
        with open("storageFileNames.txt", "r") as names_file:
            proccess_files = names_file.readlines()
            for file in proccess_files:
                #每个压缩包处理：1、解压； 2、读取其中的对应两个csv，并进行处理
                file_path = self.info_dict + file
                if not os.path.exists(file_path):
                    logger.info(u"未找到文件" + file_path + u"，请检查后另外单独执行")
                else:
                    pass
                    logger.info(u"开始解压文件" + file_path + u"...")
                    self.form_brand_record_redis(file_path)

    def check_info_valid(self, apply_date, class_no):
        check_res = True
        #检查申请时间格式
        try:
            apply_date = time.strptime(apply_date, self.date_origin_format)
            apply_date = time.strftime(self.date_target_format, apply_date)
        except:
            check_res = False

        #检查国际类别的取值
        try:
            if int(class_no) not in range(1, 46):
                check_res = False
        except:
            check_res = False
        return check_res, apply_date, class_no

    ####record表存到redis中
    def form_brand_record_redis(self, zip_file_name):
        unzip_dir_name = zip_file_name.split(".zip")[0]
        os.system("unzip " + zip_file_name.encode("utf8") + " -d " + unzip_dir_name.encode("utf8"))

        item_dict = load_brand_item()

        info_csv_name = unzip_dir_name + '/' + self.info_csv_name
        item_csv_name = unzip_dir_name + '/' + self.item_csv_name
        info_load_state, info_data = self.csv_reader.load_csv_to_pandas(info_csv_name)
        item_load_state, item_data = self.csv_reader.load_csv_to_pandas(item_csv_name)
        if info_load_state and item_load_state == False:
            logger.error(u"注意：压缩包%s中有解析失败的数据文件，已经跳过"%(zip_file_name.encode("utf8")))
            return
        else:
            logger.info(u"压缩包%s中数据文件解析成功，开始导入Redis数据库" % (zip_file_name.encode("utf8")))

            # self.redis_con.pipe.sadd(self.item_key_prefix + str(class_no) + "::" + str(b_id), product_no)
            logger.info(u"开始导入csv文件:%s... ..." % info_csv_name)
            line_num, info_ok_cnt = self.process_info_csv(info_data)
            logger.info(u"csv文件 %s 处理完毕，文件有效行总计 %d行, 导入成功行数%d" % (info_csv_name, line_num, info_ok_cnt))

            logger.info(u"开始导入csv文件:%s... ..." % info_csv_name)
            line_num, info_ok_cnt = self.process_info_csv(info_data)
            logger.info(u"csv文件 %s 处理完毕，文件有效行总计 %d行, 导入成功行数%d" % (info_csv_name, line_num, info_ok_cnt))



    def key_statistic(self):
        u"""
        对四十五大类的独立商标（不重复的《注册号+商标名》二元组）进行统计
        :return:
        """
        for class_no in range(1, 46):
            record_key = self.rank_key_prefix + str(class_no) + "::id"
            record_cnt_key = self.rank_key_prefix + str(class_no) + "::cnt"
            set_size = self.redis_con.db.hlen(record_key)
            cnt_set_size = self.redis_con.db.get(record_cnt_key)

            data_key = self.data_key_prefix + str(class_no) + "::*"
            data_key_set = self.redis_con.db.keys(data_key)
            set_data_size = len(data_key_set)

            item_key = self.item_key_prefix + str(class_no) + "::*"
            item_key_set = self.redis_con.db.keys(item_key)
            set_item_size = len(item_key_set)
            print "key %s has %d keys, while key %s has %d" % (record_key, set_size, item_key, set_item_size)

    u"""
    处理基本信息的函数
    """
    def process_info_csv(self, info_data):
        ##先处理基本信息
        line_num = info_data.shape[0]  ##csv总行数
        info_ok_cnt = 0
        for line in range(0, line_num):
            ###解析csv字段，并确定数据行的可用性
            try:
                ###解析数据行，检查取值
                brand_no = info_data[u"注册号/申请号"][line]
                apply_date = info_data[u"申请日期"][line]
                class_no = info_data[u"国际分类"][line]
                brand_status = 0 if pd.isna(info_data[u"专用期开始日期"][line]) else 1  # 专用期不为空则1

                check_res, apply_date, class_no = self.check_info_valid(apply_date, class_no)
                if not check_res:
                    logger.error(u"发现错误数据行，数据行号%d，已跳过，原因：数据行内容取值不符合预期取值的格式" %
                                 line_num)
                    continue

                ##解析数据行的商标名，是图形或者空就跳过
                brand_name = info_data[u"商标名称"][line]
                brand_name = brand_name.strip()
                if brand_name == u"图形" or pd.isna(brand_name) or len(brand_name) == 0:  # 商标名是图形的其实是图形商标
                    continue

                ##用商标名+id，按大类聚合
                ##检查大类里是否已经有了这个id+商标名组合
                hset_id_key = self.rank_key_prefix + class_no + "::id"
                hset_cnt_key = self.rank_key_prefix + class_no + "::cnt"
                bkey = brand_no + "," + brand_name
                if not self.redis_con.db.hget(hset_id_key, bkey):
                    ###只有不在集合里，才生成并更新
                    ##将他先加入某个大类，分配一个id，然后还要存储它的数据、构造读音集合等
                    b_id = self.redis_con.db.incr(hset_cnt_key)
                    self.redis_con.db.hset(hset_id_key, bkey, b_id)
                    info_ok_cnt += 1
                    self.add_new_brand(brand_name, brand_no, brand_status, apply_date, class_no, b_id, line)

                info_ok_cnt += 1
            except Exception, e:
                logger.error(u"将第%d行数据导入数据库时发生错误，原因：" % line)
                traceback.format_exc()
        return line_num, info_ok_cnt

    def reset_redis_data(self):
        ##清理数据库原有的redis数据
        self.redis_con.clear_redis_key(self.data_key_prefix)
        self.redis_con.clear_redis_key(self.item_key_prefix)
        self.redis_con.clear_redis_key(self.py_key_prefix)
        self.redis_con.clear_redis_key(self.rank_key_prefix)

    ###在redis数据库中增加一个新商标的相关数据
    def add_new_brand(self, brand_name, brand_no, brand_status, apply_date, class_no, b_id, line_no):
        ##将商标名分解为中文、英文、数字，中文转拼音，英文分成词，并把拼音和英文词合并
        brand_china = brand.get_china_str(brand_name)
        brand_pinyin = lazy_pinyin(brand_china, style=Style.TONE3)
        brand_num, brand_eng = brand.get_not_china_list(brand_name)

        record_dict = {
            "bid": b_id,
            "name": brand_name,
            "no": brand_no,
            "sts": brand_status,
            "py": ','.join(brand_pinyin),
            "ch": brand_china,
            "eng": ','.join(brand_eng),
            "num": ','.join(brand_num),
            "date": apply_date
        }
        ###存储数据
        data_key = self.data_key_prefix + str(class_no) + "::" + str(b_setid)
        #print data_key
        #print record_dict
        self.redis_con.pipe.hmset(data_key, record_dict)

        ###存储拼音/英文字集合
        brand_py_unit = []
        brand_py_unit.extend(brand_pinyin)
        brand_py_unit.extend(brand_eng)
        brand_py_unit.extend(brand_num)
        if len(brand_py_unit) > 0:
            ##构造1~3元组合
            for combi_low in range(1, 3):
                combi_set = combinations(brand_py_unit, combi_low)
                ###对每种组合都存一个集合
                for combi in combi_set:
                    set_key = self.py_key_prefix + str(class_no) + "::" + ','.join(combi) ##key = "bPySet::1::ni2,hao3" ,类似这样的
                    #print set_key
                    self.redis_con.pipe.sadd(set_key, b_id)
        else:
            logger.debug(u"出现不含中文、英文与数字的商标，商标名：brand %s ,line_no = %d"%(brand_name, line_no))
        ##批量插入
        self.redis_con.pipe.execute()


##975418个不同的商标，12277622
if __name__=="__main__":
    data_storage = DataStorage(clean_out=True)
    #form_brand_record_redis()
    #load_brand_item()






