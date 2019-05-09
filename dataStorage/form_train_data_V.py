#-*-coding:utf8-*-#
from brand_item import BrandItem
import time
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
from similarity import strFunction
from brand_train_data import BrandTrainData

class TrainDataFormer:

    rank_key_prefix = "bRank::"
    data_key_prefix = "bData::"
    py_key_prefix = "bPySet::"  # set
    item_key_prefix = "bItem::"

    limit = [
        {"func": lambda x: x < "2015", "cnt":500, "bcnt":10, "upb": "2015"},
        {"func": lambda x: "2015" < x < "2017", "cnt": 500, "bcnt": 10, "upb": "2017"},
        {"func": lambda x: "2017" < x < "201811", "cnt": 500, "bcnt": 10, "upb": "201811"}
             ]

    u""" 训练数据 mysql表:
        待查商标名、待查商标注册号、待查商标申请时间、待查商标申请状态
        历史商标名、历史商标注册号、历史商标申请时间、历史商标申请状态
        国际分类
        待查商标与历史商标的相似计算结果（list转json串）
     """

    def __init__(self, store_mysql=False):
        self.redis_con = RedisConnection()
        self.csv_reader = CsvReader()
        self.item_dict = self.load_brand_item()
        #构造训练数据
        self.form_brand_record_redis(store_mysql)

    def get_limit_loc(self, apply_date):
        loc = 0
        for idx in range(len(self.limit)):
            f = self.limit[idx]["func"]
            if f(apply_date): # 申请日期满足限定条件
                loc = idx
                break
        return  loc


    def check_info_valid(self, brand_name, apply_date, date_limit="3000"):
        u""" 检查对应的数据段是否满足取用的要求 """
        if apply_date >= date_limit:
            return False

        if len(brand_name) * 3 > 64:
            return False

        return True

    ####record表存到redis中
    def form_brand_train_data(self, store_mysql):
        u"""
        依照大类遍历每个大类下的所有数据。以被选中的商标作为“待查商标”，每个大类各选1000个注册成功/失败的待查商标（最多各500个15年以前的）
         --也就是最多9W个。每个商标最多取10个近似商标，近似商标除了相似度满足要求外，还要申请时间<待查商标
        """
        unzip_dir_name = zip_file_name.split(".zip")[0].replace(" ", "")
        os.system("unzip -o '%s'  -d  '%s'" % (zip_file_name.encode("utf8"), unzip_dir_name.encode("utf8")))

        info_csv_name = unzip_dir_name + '/' + self.info_csv_name
        item_csv_name = unzip_dir_name + '/' + self.item_csv_name
        info_load_state, info_data = self.csv_reader.load_csv_to_pandas(info_csv_name)
        item_load_state, item_data = self.csv_reader.load_csv_to_pandas(item_csv_name)
        if info_load_state and item_load_state == False:
            logger.error(u"注意：压缩包%s中有解析失败的数据文件，已经跳过"%(zip_file_name.encode("utf8")))
            return
        else:
            logger.info(u"压缩包%s中数据文件解析成功，开始导入Redis数据库" % (zip_file_name.encode("utf8")))

            logger.info(u"开始导入csv文件:%s... ..." % info_csv_name)
            line_num, info_ok_cnt, info_invalid_cnt, info_skip_cnt, info_unique_cnt, info_error_cnt \
                = self.process_info_csv(info_data, store_mysql)
            logger.info(u"csv文件 %s 处理完毕，文件有效行总计 %d行, 导入成功行数%d，"
                        u"数据行不合法%d行，图形商标或无名字商标%d行，重复的注册号%d, 插入数据出错%d行" %
                        (info_csv_name, line_num, info_ok_cnt, info_invalid_cnt, info_skip_cnt, info_unique_cnt, info_error_cnt))

            logger.info(u"开始导入csv文件:%s... ..." % item_csv_name)
            line_num, item_ok_cnt, item_invalid_class_cnt, item_invalid_group_cnt, item_invalid_product_cnt\
                , item_miss_cnt = self.process_item_csv(item_data)
            logger.info(u"csv文件 %s 处理完毕，文件有效行总计 %d行, 导入成功行数%d，"
                        u"数据行不合法行：（国际类别不合法%d行，类似群不合法%d行，商品项不在尼斯文件内%d行）"
                        u"，另外还有对应的注册号不在库中的数据%d行" %
                        (item_csv_name, line_num, item_ok_cnt, item_invalid_class_cnt, item_invalid_group_cnt,
                         item_invalid_product_cnt, item_miss_cnt))

            logger.info(u"压缩包%s中的信息已导入完毕，导入后的数据分布为：")
            self.key_statistic()

    def key_statistic(self):
        u"""
        对四十五大类的独立商标（不重复的《注册号+商标名》二元组）进行统计
        :return:
        """
        for class_no in range(1, 46):
            record_key = self.rank_key_prefix + str(class_no) + "::id"
            record_cnt_key = self.rank_key_prefix + str(class_no) + "::cnt"
            set_size = self.redis_con.db.hlen(record_key)
            set_size = int(set_size) if set_size else 0
            cnt_set_size = self.redis_con.db.get(record_cnt_key)
            cnt_set_size = int(cnt_set_size) if cnt_set_size else 0

            data_key = self.data_key_prefix + str(class_no) + "::*"
            data_key_set = self.redis_con.db.keys(data_key)
            set_data_size = len(data_key_set)

            item_key = self.item_key_prefix + str(class_no) + "::*"
            item_key_set = self.redis_con.db.keys(item_key)
            set_item_size = len(item_key_set)
            logger.info(u"第 %d 大类总计有 %d 个不同的注册号（计数量%d), "\
                        u"对应的数据存储量%d, 商品项表%d"
                        % (class_no, set_size, cnt_set_size, set_data_size, set_item_size))

    u"""
    处理基本信息的函数
    """
    def process_info_csv(self, info_data, store_mysql):
        ##先处理基本信息

        info_ok_cnt = 0
        info_invalid_cnt = 0
        info_skip_cnt = 0
        info_unique_cnt = 0
        info_error_cnt = 0
        batch = 100000
        insert_list = []
        old = 0
        init_time = time.time()  # 导入开始时间
        init_redis_time = time.time()
        init_mysql_time = time.time()
        delta_redis_time = 0.
        delta_mysql_time = 0.
        db = self.redis_con.db
        cnt_res = []
        for class_no in range(1, 46):
            cnt_res.append([[0, 0]] * len(self.limit)) # 每个大类里为每种预定义类型找的通过/不通过（待查）商标计数
            idkey = self.rank_key_prefix + "%d::cnt" % (class_no)
            idkey = int(idkey)
            for id in range(idkey):
                if line % batch == 0:
                    logger.info(u"数据导入中，处理进度%d/%d" % (line, line_num))
                    ##批量插入
                    init_redis_time = time.time()
                    self.redis_con.pipe.execute()
                    init_mysql_time, delta_redis_time = self.compute_time_seg(init_redis_time, delta_redis_time, "redis", output=True)
                    if store_mysql == True:
                        logger.info(u"mysql 插入行数 %d" % (len(insert_list)))
                        db_session.add_all(insert_list)
                        db_session.commit()
                        del insert_list[:]
                        _, delta_mysql_time = self.compute_time_seg(init_mysql_time, delta_mysql_time, "mysql", output=True)

                data_key = self.data_key_prefix + "%d::%d"
                info_data = db.hgetall(data_key)

                ###解析数据行，检查取值
                brand_no = info_data["no"]
                apply_date = info_data["date"]
                brand_name = info_data["name"]
                brand_status = int(info_data["sts"])
                loc = self.get_limit_loc(apply_date)
                check_res = self.check_info_valid(apply_date, class_no)

                if not check_res:
                    continue
                    info_invalid_cnt += 1
                    logger.error(u"发现错误数据行，数据行号%d，已跳过，原因：数据行内容取值不符合预期取值的格式,"
                                 u"apply_date=%s, class_no = %s" %
                                 (line, apply_date, class_no))
                    continue

                ##解析数据行的商标名，是图形或者空就跳过
                brand_name = info_data[u"商标名称"][line]
                if brand_name == u"图形" or pd.isna(brand_name) or len(brand_name.strip()) == 0:  # 商标名是图形的其实是图形商标
                    info_skip_cnt += 1
                    brand_name = str(brand_name)
                    insert_state = 2
                else:
                    brand_name = brand_name.strip()
                    ##用id，按大类聚合
                    ##检查大类里是否已经有了这个id
                    u""" redis操作 """
                    init_redis_time = time.time()
                    hset_id_key = self.rank_key_prefix + class_no + "::id"
                    hset_cnt_key = self.rank_key_prefix + class_no + "::cnt"
                    insert_state = 1
                    b_id = self.redis_con.db.hget(hset_id_key, brand_no)
                    if not b_id:
                        ###只有不在集合里，才生成并更新
                        ##将他先加入某个大类，分配一个id，然后还要存储它的数据、构造读音集合等
                        b_id = self.redis_con.db.incr(hset_cnt_key)
                        self.redis_con.db.hset(hset_id_key, brand_no, b_id)
                    else: ###重复的，只考虑更新商标状态和日期
                        info_unique_cnt += 1
                        insert_state = 4

                    if not self.redis_con.db.hget(self.data_key_prefix + str(class_no) + "::" + str(b_id), "bid"):
                        if self.add_new_brand(brand_name, brand_no, brand_status, apply_date, class_no, b_id, line):
                            info_skip_cnt += 1
                            insert_state = 3
                        else:
                            info_ok_cnt += 1
                    u""" redis操作结束 """
                    _, delta_redis_time = self.compute_time_seg(init_redis_time, delta_redis_time, "redis",
                                                                              output=False)
                if store_mysql == True:##是否转存数据库
                    init_mysql_time = time.time()
                    if insert_state == 4:#更新
                        pass
                        update_record = db_session.query(BrandHistory).filter(BrandHistory.brand_no == brand_no).first()
                        if update_record:
                            update_record.brand_status = brand_status
                            update_record.apply_date = apply_date
                        else:
                            update_record = BrandHistory(brand_no, brand_name, apply_date, int(class_no), brand_status,
                                              insert_state)
                        insert_list.append(update_record)
                    else:
                        record = db_session.query(BrandHistory).filter(BrandHistory.brand_no == brand_no).first()
                        if not record:
                            new_record = BrandHistory(brand_no, brand_name, apply_date, int(class_no), brand_status
                                                      , insert_state)
                            insert_list.append(new_record)
                    u""" mysql操作结束 """
                    _, delta_mysql_time = self.compute_time_seg(init_mysql_time, delta_mysql_time, "mysql",
                                                                              output=False)
            except Exception, e:
                info_error_cnt += 1
                logger.error(u"将第%d行数据导入数据库时发生错误，原因：" % line)
                logger.error(traceback.format_exc())
                try:
                    test_redis = self.redis_con.db.get(self.rank_key_prefix + "1::id")
                except:
                    logger.error(u"reids数据库崩溃，不可继续存储，请检查内存空间是否足够")
                    logger.error(traceback.format_exc())
                    break

        ##批量插入
        init_redis_time = time.time()
        self.redis_con.pipe.execute()
        init_mysql_time, delta_redis_time = self.compute_time_seg(init_redis_time, delta_redis_time, "redis",
                                                                  output=True)
        if store_mysql == True:
            logger.info(u"mysql 插入行数 %d" % (len(insert_list)))
            db_session.add_all(insert_list)
            db_session.commit()
            del insert_list[:]
            _, delta_mysql_time = self.compute_time_seg(init_mysql_time, delta_mysql_time, "mysql", output=True)
        ##总时间消耗
        _, __ = self.compute_time_seg(init_time, 0, "all", output=True)
        return line_num, info_ok_cnt, info_invalid_cnt, info_skip_cnt, info_unique_cnt, info_error_cnt

    ###在redis数据库中增加一个新商标的相关数据
    def add_new_brand(self, brand_name, brand_no, brand_status, apply_date, class_no, b_id, line_no):
        ##将商标名分解为中文、英文、数字，中文转拼音，英文分成词，并把拼音和英文词合并
        brand_china = strFunction.get_china_str(brand_name)
        brand_pinyin = lazy_pinyin(brand_china, style=Style.TONE3)
        brand_num, brand_eng, brand_letters = strFunction.get_not_china_list(brand_name)

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
        data_key = self.data_key_prefix + str(class_no) + "::" + str(b_id)
        #print data_key
        #print record_dict
        self.redis_con.pipe.hmset(data_key, record_dict)

        cnt_skip = 0
        ###存储拼音/英文字集合
        brand_py_unit = []
        brand_py_unit.extend(brand_pinyin)
        #brand_py_unit.extend(brand_eng)
        brand_py_unit.extend(brand_letters)
        brand_py_unit.extend(brand_num)
        if len(brand_py_unit) > 0:
            ###对每个单字都存一个集合
            for py in brand_py_unit:
                set_key = self.py_key_prefix + str(class_no) + "::" + py ##key = "bPySet::1::ni2" ,类似这样的
                #print set_key
                self.redis_con.pipe.sadd(set_key, b_id)
        else:
            cnt_skip = 1
            logger.debug(u"出现不含中文、英文与数字的商标,或者只包含无法解析的字体/符号，商标名：brand %s ,line_no = %d"%(brand_name, line_no))
        return cnt_skip

    def load_brand_item(self):
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
        return item_dict

    def compute_time_seg(self, start, delta, name, output=False):
        end = time.time()
        delta = delta + (end - start)
        if output:
            logger.info(u"%s处理一个batch耗时 %.f分%.f秒" % (name, delta//60, delta%60))
        return end, delta


##975418个不同的商标，12277622
if __name__=="__main__":
    data_storage = DataStorage(clean_out=False, store_mysql=True)







