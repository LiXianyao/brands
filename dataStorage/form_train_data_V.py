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
from similarity import strFunction, brand, compute
import numpy as np

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
        self.gate = ['C', 0.67, 'C', 'C', 'N', 0.67, 0.67, 'C', 'C', 1.0]
        #构造训练数据
        self.form_brand_record_redis(store_mysql)

    def get_limit_loc(self, apply_date):
        loc = 0
        for idx in range(len(self.limit)):
            f = self.limit[idx]["func"]
            if f(apply_date): # 申请日期满足限定条件
                loc = idx
                break
        return  loc, self.limit[loc]["cnt"], self.limit[loc]["bcnt"]


    def check_info_valid(self, brand_name, apply_date, cnt=0, cnt_limit=1, date_limit="3000"):
        u""" 检查对应的数据段是否满足取用的要求 """
        if apply_date >= date_limit:
            return False

        if len(brand_name) * 3 > 64:
            return False

        if cnt >= cnt_limit:
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
        cnt_res = np.zeros([46, len(self.limit), 2], dtype=int)
        for class_no in range(1, 46):
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
                loc, cnt_limit, b_limit = self.get_limit_loc(apply_date)
                check_res = self.check_info_valid(brand_name, apply_date, cnt=cnt_res[class_no][loc][brand_status], cnt_limit=cnt_limit)

                if not check_res:
                    u"""商标长度不合格或者计数已经够了"""
                    continue

                u""" 获取商标的拼音+英文字符集，准备查询 """
                brand_name_china = strFunction.get_china_str(brand_name)
                brand_name_pinyin = lazy_pinyin(brand_name_china, style=Style.TONE3)
                brand_name_num, brand_name_eng, character_set = strFunction.get_not_china_list(brand_name)
                brand_name_pinyin.extend(brand_name_eng)

                similar_cnt = 0
                last_class = {}
                compare_list = self.get_pysimilar_unit(brand_name_pinyin, db, class_no)
                cnt_b = np.zeros([2], dtype=int)  # 对当前这个待查商标的近似商标的计数
                if compare_list: #非空，即找到了近似商标
                    for i in range(len(compare_list)):
                        compare_unit = compare_list[i]
                        his_apply_date = compare_unit["date"]
                        his_name = compare_unit["name"]
                        his_brand_sts = compare_unit["sts"]
                        # 检查申请日期 < 待查商标，商标名长度
                        check_res = self.check_info_valid(his_name, his_apply_date, date_limit=apply_date)
                        if not check_res:
                            continue

                        his_name_pinyin = compare_unit["py"]
                        his_name_china = compare_unit["ch"]
                        his_brand_no = compare_unit["no"]
                        his_name_eng = compare_unit["eng"]
                        his_name_pinyin = self.concate(his_name_pinyin, his_name_eng)
                        last_class[class_no] = compare_unit
                        if not compute.judge_pinyin(brand_name_pinyin, his_name_pinyin):
                            if len(brand_name_china) != len(his_name_china) or brand.glyphApproximation(
                                    brand_name_china, his_name_china) < 0.9:
                                continue
                        # 计算相似度
                        # print "brand %s, his%s, class %d"%(brand_name, his_name, class_no)
                        similar, compare_Res = compute.compute_similar(brand_name, his_name, self.gate)
                        similar_loc = 0
                        if similar == True:  # 有某项相似度较高，记为相似度高的记录
                            similar_loc = 1
                        check_res = self.check_info_valid(his_name, his_apply_date,
                                                          cnt=cnt_b[his_brand_sts], cnt_limit=b_limit,
                                                          date_limit=apply_date)
                        if not check_res:
                            continue
                            out_row = [brand_name.encode("gbk"), his_name.encode("gbk"), brand_no, his_brand_no,
                                       class_no, compare_Res, brand_status, compare_unit["sts"], '1']
                            writer.writerow(out_row)
                            similar_cnt += 1
                    del compare_list

    u""" 返回中文与英文部分的拼接串 """
    def concate(self, his_name_pinyin, his_name_eng):
        if len(his_name_pinyin) > 0:
            if len(his_name_eng) > 0:
                his_name_pinyin = his_name_pinyin + "," + his_name_eng
        elif len(his_name_eng) > 0:
            his_name_pinyin = his_name_eng
        return his_name_pinyin

    def get_pysimilar_unit(self, brand_name_pinyin, db, class_no):
        py_low = compute.compute_py_lowb(brand_name_pinyin)  ##根据长度确定确定排列组合的下界
        py_combi = combinations(brand_name_pinyin, py_low)
        if py_low > 0:
            # 共有拼音排列组合
            union = set()
            for combi in py_combi:
                if len(combi) == 1:
                    inter = db.smembers(self.py_key_prefix + str(class_no) + "::" + combi[0])
                    # s = combi[0]
                else:
                    ###多元组，将redis中多个集合合并
                    inter, s = db.get_pycombi(combi, class_no)
                union = union | inter
                # print "class = %d,py combi %s has %d"%(class_no, s, len(inter))
            compare_list = db.get_union_data(class_no, union)
            return compare_list
        else:  ###没有汉字没有英文没有数字
            return []

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







