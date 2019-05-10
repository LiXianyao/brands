#-*-coding:utf8-*-#
import time
import traceback
from storage_connection import RedisConnection
from brand_train_data import BrandTrainData, db_session
import sys
from pypinyin import lazy_pinyin, Style
from itertools import combinations
import json
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append('..')
from consoleLogger import logger
from similarity import strFunction, brand, compute
import numpy as np

class TrainDataFormer:

    rank_key_prefix = "bRank::"
    data_key_prefix = "bData::"
    py_key_prefix = "bPySet::"  # set
    item_key_prefix = "bItem::"

    limit = [
        {"func": lambda x: x < "2015", "cnt":5, "bcnt":10, "upb": "2015"},
        {"func": lambda x: "2015" < x < "2017", "cnt": 5, "bcnt": 10, "upb": "2017"},
        {"func": lambda x: "2017" < x < "201811", "cnt": 5, "bcnt": 10, "upb": "201811"}
             ]

    u""" 训练数据 mysql表:
        待查商标名、待查商标注册号、待查商标申请时间、待查商标申请状态
        历史商标名、历史商标注册号、历史商标申请时间、历史商标申请状态
        国际分类
        待查商标与历史商标的相似计算结果（list转json串）
     """

    def __init__(self, store_file=False, store_mysql=False):
        self.redis_con = RedisConnection()
        self.gate = ['C', 0.67, 'C', 'C', 'N', 0.67, 0.67, 'C', 'C', 1.0]
        self.store_batch = 100
        self.delta_redis_time = 0.
        self.delta_mysql_time = 0.
        #构造训练数据
        self.form_train_data(store_mysql)

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

    u"""
    处理基本信息的函数
    """
    def form_train_data(self, store_mysql):
        ##先处理基本信息
        insert_list = []
        db = self.redis_con.db
        cnt_res = np.zeros([46, len(self.limit), 2], dtype=int)
        cnt_suc = np.zeros([46], dtype=int)
        cnt_b_suc = np.zeros([46, 2], dtype=int)
        for class_no in range(1, 46):
            idkey = self.rank_key_prefix + "%d::cnt" % (class_no)
            idcnt = int(db.get(idkey))
            for idx in range(idcnt):
                self.batch_store(cnt_suc, cnt_b_suc, store_mysql, insert_list)
                data_key = self.data_key_prefix + "%d::%d"%( class_no, idx)
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

                compare_list = self.get_pysimilar_unit(brand_name_pinyin, db, class_no)
                cnt_b = np.zeros([2], dtype=int)  # 对当前这个待查商标的近似商标的计数
                if compare_list: #非空，即找到了近似商标
                    train_data_cache = []
                    for i in range(len(compare_list)):
                        compare_unit = compare_list[i]
                        his_apply_date = compare_unit["date"]
                        his_name = compare_unit["name"]
                        his_brand_sts = int(compare_unit["sts"])
                        # 检查申请日期 < 待查商标，商标名长度
                        check_res = self.check_info_valid(his_name, his_apply_date, date_limit=apply_date)
                        if not check_res:
                            continue

                        his_name_pinyin = compare_unit["py"]
                        his_name_china = compare_unit["ch"]
                        his_brand_no = compare_unit["no"]
                        his_name_eng = compare_unit["eng"]
                        his_name_pinyin = self.concate(his_name_pinyin, his_name_eng)
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
                        u""" 相似度高的样本、相似度低的样本各取b个 """
                        check_res = self.check_info_valid(his_name, his_apply_date,
                                                          cnt=cnt_b[similar_loc], cnt_limit=b_limit,
                                                          date_limit=apply_date)
                        if not check_res:
                            continue
                        similarity = json.dumps(compare_Res)
                        train_data = BrandTrainData(brand_no, brand_name,brand_status, apply_date, class_no, his_brand_no,
                                                    his_name, his_brand_sts, his_apply_date, similarity)
                        train_data_cache.append(train_data)
                        cnt_b[similar_loc] += 1

                    u""" 几个计数值的修改 """
                    # 训练数据太少，不要了
                    if len(train_data_cache) < 3 or not len(cnt_b[1]):
                        continue
                    insert_list.extend(train_data_cache)
                    cnt_res[class_no][loc][brand_status] += 1
                    cnt_suc[class_no] += 1
                    cnt_b_suc[class_no] += cnt_b
                    del compare_list
                u""" 某一类的商标数达到目标则结束这个类别的检索 """
                if np.sum(cnt_res[class_no][loc]) == 2 * cnt_limit:
                    break
            class_suc_cnt = np.sum(cnt_res[class_no], axis=0)
            logger.info(u"国际分类%d的商标检索已结束，共计提取样本%d个，其中%d个通过商标样本和%d个不通过商标样本" % (class_no,cnt_suc[class_no], class_suc_cnt[1], class_suc_cnt[class_no][0]))
            logger.info(u"对应的近似度高商标和近似度低商标分别有%d个 和 %d个"%(cnt_b_suc[class_no][1], cnt_b_suc[class_no][0]))
            del idkey[:]
            self.batch_store(cnt_suc, cnt_b_suc, store_mysql, insert_list)
        self.batch_store(cnt_suc, cnt_b_suc, store_mysql, insert_list, force=True)

    u""" 按条件，批量存储数据 """
    def batch_store(self, cnt_suc, cnt_b_suc, store_mysql, insert_list, force=False):
        cur_suc = np.sum(cnt_suc)
        cur_b_suc = np.sum(cnt_b_suc)
        if cur_suc % self.store_batch == 0 or force:
            logger.info(u"训练数据构造中，已检索到%d个满足要求的实例，生成训练样本%d个" % (cur_suc, cur_b_suc))
            ##批量插入
            init_redis_time = time.time()
            self.redis_con.pipe.execute()
            init_mysql_time, delta_redis_time = self.compute_time_seg(init_redis_time, self.delta_redis_time, "redis",
                                                                      output=True)
            if store_mysql == True:
                logger.info(u"mysql 插入行数 %d" % (len(insert_list)))
                db_session.add_all(insert_list)
                db_session.commit()
                del insert_list[:]
                _, delta_mysql_time = self.compute_time_seg(init_mysql_time, self.delta_mysql_time, "mysql", output=True)

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

    def compute_time_seg(self, start, delta, name, output=False):
        end = time.time()
        delta = delta + (end - start)
        if output:
            logger.info(u"%s处理一个batch耗时 %.f分%.f秒" % (name, delta//60, delta%60))
        return end, delta



##975418个不同的商标，12277622
if __name__=="__main__":
    train_data_former = TrainDataFormer(store_file=False, store_mysql=True)







