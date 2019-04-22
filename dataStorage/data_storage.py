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
from processdata.brand_history import BrandHistory, db_session

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

    def __init__(self, clean_out=False, store_mysql=False):
        self.redis_con = RedisConnection()
        self.csv_reader = CsvReader()
        self.item_dict = self.load_brand_item()
        if clean_out:
            logger.info(u"数据库重置开启，开始清洗数据库")
            self.reset_redis_data()
            logger.info(u"数据库清洗完毕")
        #读取要转储的压缩包文件名
        with open("storageFileNames.txt", "r") as names_file:
            proccess_files = names_file.readlines()
            for file in proccess_files:
                file = file.strip()
                #每个压缩包处理：1、解压； 2、读取其中的对应两个csv，并进行处理
                file_path = self.info_dict + file
                if not os.path.exists(file_path):
                    logger.info(u"未找到文件" + file_path + u"，请检查后另外单独执行")
                else:
                    pass
                    logger.info(u"开始解压文件" + file_path + u"...")
                    self.form_brand_record_redis(file_path, store_mysql)

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
    def form_brand_record_redis(self, zip_file_name, store_mysql):
        unzip_dir_name = zip_file_name.split(".zip")[0]
        os.system("unzip -o " + zip_file_name.encode("utf8") + " -d  " + unzip_dir_name.encode("utf8"))

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
    处理商品服务信息的函数
    """
    def process_item_csv(self, item_data):
        ##先处理基本信息
        line_num = item_data.shape[0]  ##csv总行数
        item_ok_cnt = 0
        item_invalid_class_cnt = 0
        item_invalid_group_cnt = 0
        item_invalid_product_cnt = 0
        item_miss_cnt = 0
        batch = 500000
        for line in range(0, line_num):
            if line % batch == 0:
                self.redis_con.pipe.execute()
                logger.info(u"数据导入中，处理进度%d/%d" % (line, line_num))
            ###解析csv字段，并确定数据行的可用性
            ###解析数据行，检查取值
            brand_no = item_data[u"注册号/申请号"][line]
            group_no = item_data[u"类似群"][line]
            class_no = item_data[u"国际分类"][line]
            item_name = item_data[u'商品中文名称'][line]

            ##无效数据跳过
            if pd.isna(class_no) or (int(class_no) < 1 and int(class_no) > 45):
                item_invalid_class_cnt += 1
                logger.debug(u"第%d行数据解析无效，已跳过，原因：国际分类编码不在区间内"%line)
                continue
            if pd.isna(group_no) or len(group_no) != 4:
                item_invalid_group_cnt += 1
                #logger.debug(u"第%d行数据解析无效，已跳过，原因：类似群号为空或不在范围内"%line)
                continue
            try:
                product_no = self.item_dict[int(group_no)][item_name]
            except:
                item_invalid_product_cnt += 1
                #logger.debug(u"第%d行数据解析无效，已跳过，原因：无效的群组号%s"\
                #             u"或商品中文名称%s不在尼斯文件规定的商品项内" % (line, str(group_no), str(item_name)))
                continue

            ##取到这个注册号的bid
            hset_id_key = self.rank_key_prefix + class_no + "::id"
            b_id = self.redis_con.db.hget(hset_id_key, brand_no)
            if not b_id:
                item_miss_cnt += 1
                continue
            item_ok_cnt += 1
            self.redis_con.pipe.sadd(self.item_key_prefix + str(class_no) + "::" + str(b_id), product_no)
        self.redis_con.pipe.execute()
        return line_num, item_ok_cnt, item_invalid_class_cnt, item_invalid_group_cnt, item_invalid_product_cnt, item_miss_cnt

    u"""
    处理基本信息的函数
    """
    def process_info_csv(self, info_data, store_mysql):
        ##先处理基本信息
        line_num = info_data.shape[0]  ##csv总行数
        info_ok_cnt = 0
        info_invalid_cnt = 0
        info_skip_cnt = 0
        info_unique_cnt = 0
        info_error_cnt = 0
        batch = 100000
        insert_list = []
        old = 0
        for line in range(0, line_num):
            if line < old:
                continue
            if line % batch == 0:
                logger.info(u"数据导入中，处理进度%d/%d" % (line, line_num))
                ##批量插入
                self.redis_con.pipe.execute()
                if store_mysql == True:
                    logger.info(u"mysql 插入行数 %d" % (len(insert_list)))
                    db_session.add_all(insert_list)
                    db_session.commit()
                    del insert_list[:]
            ###解析csv字段，并确定数据行的可用性
            try:
                ###解析数据行，检查取值
                brand_no = info_data[u"注册号/申请号"][line]
                apply_date = info_data[u"申请日期"][line]
                class_no = info_data[u"国际分类"][line]
                brand_status = 0 if pd.isna(info_data[u"专用期开始日期"][line]) else 1  # 专用期不为空则1

                check_res, apply_date, class_no = self.check_info_valid(apply_date, class_no)
                if not check_res:
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

                if store_mysql == True:##是否转存数据库
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
            self.redis_con.pipe.execute()
            if store_mysql == True:
                db_session.add_all(insert_list)
                db_session.commit()
                del insert_list[:]
        return line_num, info_ok_cnt, info_invalid_cnt, info_skip_cnt, info_unique_cnt, info_error_cnt

    def reset_redis_data(self):
        ##清理数据库原有的redis数据
        self.redis_con.clear_redis_key(self.data_key_prefix)
        self.redis_con.clear_redis_key(self.item_key_prefix)
        self.redis_con.clear_redis_key(self.py_key_prefix)
        self.redis_con.clear_redis_key(self.rank_key_prefix)

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

##975418个不同的商标，12277622
if __name__=="__main__":
    data_storage = DataStorage(clean_out=False, store_mysql=True)







