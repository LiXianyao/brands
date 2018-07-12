#-*-coding:utf8-*-#
import sys
import  ConfigParser
import redis
import os
from pypinyin import lazy_pinyin
import time
from itertools import combinations
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )
cf = ConfigParser.ConfigParser()
cf.read("redis.config")
redis_ip = cf.get("redis","redis_ip")
redis_port = cf.get("redis","redis_port")
redis_db = cf.get("redis","redis_db")
redis_pwd = cf.get("redis","redis_pwd")

from processdata.database import db_session
from processdata.brand_item import BrandItem
from similarity import brand
import csv
import time
import json
import  traceback

###日期格式转换字符串
date_origi_format = "%Y年%m月%d日"
date_target_format = "%Y%m%d"

###获取商标的群组和小项名字、编号的映射关系
def load_brand_item():
    item_list = BrandItem.query.all()
    item_dict = {}
    for item in item_list:
        group_no = int(item.group_no)
        item_name = item.item_name
        item_no = item.item_no
        class_no = int(item.class_no)
        if group_no not in item_dict:
            item_dict[group_no] = {"class_no" : class_no}
        item_dict[group_no][item_name] = item_no

    #for item in item_dict[101]:
    #    print item, item_dict[101][item]
    #print item_dict.keys()
    return item_dict

###获取小项列表中的所有小项（可能有重复）
def get_product_no_list(product_list_array, item_dict):
    product_no_set = set()
    class_no_set = set()
    for index in range(len(product_list_array)):
        product_name = product_list_array[index]['product_name'].replace(u'（', u'(').replace(u'）', u')')
        product_group = int(product_list_array[index]['product_group'])
        try:
            ##确定小项所属大类
            class_no = item_dict[product_group]["class_no"]
            ##确定小项编号
            product_no = item_dict[product_group][product_name]
            ##(大类号，小项号) 加入集合（去重）
            product_no_set.add((class_no, product_no))
            class_no_set.add(class_no)
        except:
            ###现在的小项编号表里没有这个小项
            pass
    return class_no_set, product_no_set

###解析csv数据行最后三项：申请日期，大类，商标状态
def analysis_row_end(line):
    brand_no = line[2]
    apply_date = time.strptime(line[-3], date_origi_format)
    apply_date = time.strftime(date_target_format, apply_date)
    i18n_type = int(line[-2])
    brand_status = line[-1]
    return brand_no, apply_date, i18n_type, brand_status

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


###redis数据库
def form_train_data_redis(lowb, num):
    row_len = 9
    file_names = range(1, 14)##其实就是从a.csv到b.csv里面获取数据

    item_dict = load_brand_item()
    db = redis.StrictRedis(host=redis_ip, port=redis_port, db=redis_db, password=redis_pwd)
    _pipe = db.pipeline()

    old = 0
    # 中文编辑距离(越大越近)， 拼音编辑距离（越大越近）， 包含被包含（越大越近）
    # 排列组合（越大越近）， 中文含义近似（越大越近）， 中文字形近似（越大越近）
    # 英文编辑距离(越大越近)， 英文包含被包含（越大越近）， 英文排列组合（越大越近）
    # 数字完全匹配（越大越近）
    gate = ['C', 0.85, 'C', 'C', 'N', 0.9, 0.8, 'C', 'C',1.0]
    record_key = "rd::"
    record_key_time = "rdt::"
    rset_key_prefix = "rset::"
    detail_key_prefix = "dtl::"

    csv_name = u"data/origin/train_7_6_1.csv"##训练数据保存出来的文件名（个人习惯按日期命名）
    title = []
    if os.path.exists(csv_name) == False:
        ###文件不存在，则把表头写一下
        title = [u"输入商标名",u"历史商标名",u"输入商标编号",u"历史商标编号",u"所属大类",u"汉字编辑距离相似度",u"拼音相似度",u"汉字包含被包含",
                 u"汉字排列组合",u"汉字含义相近",u"汉字字形相似度",u"英文编辑距离相似度",u"英文包含被包含",
                 u"英文排列组合",u"数字完全匹配", u"商标状态", u"历史商标状态", u"是否相似商标"]
        for i in range(len(title)):
            title[i] = title[i].encode("gbk")
    f_out = open(csv_name, "a")
    writer = csv.writer(f_out)
    if len(title) != 0:
        writer.writerow(title)

    ###获得所有的历史商标, 结果结构为 申请时间 -》 【商标名，商标编号，商标状态】
    record_id_dict, record_time_dict = getHistoryBrandWithTime(record_key_time, db)
    print "history brand ready"

    session = db_session()
    cnt_status = {'0':0, "1":0}
    for file_name in file_names:
        file_name = str(file_name) + ".csv"
        with open("/home/lab/brands/csv3/" + file_name, "rU") as csv_file:
            reader = csv.reader(csv_file, delimiter=',', quotechar='\n')
            line_cnt = 0
            for line in reader:
                line_cnt += 1
                if line_cnt == 1:
                    continue
                if len(line) < row_len:
                    #print "line %d error, as:"%(line_cnt)
                    #print line
                    continue

                ###解析csv字段，并确定数据行的可用性
                try:
                    ###解析数据行尾部
                    #s_time = time.strftime("%Y%m%d%H%M%S", time.localtime())
                    brand_no, apply_date, i18n_type, brand_status = analysis_row_end(line)
                    if apply_date < '2015':
                        #pass
                        continue
                    if len(brand_status) != 1:
                        continue
                    if cnt_status[brand_status] < lowb or cnt_status[brand_status] >= (lowb + num):
                        continue
                    #print apply_date
                    ###解析小项列表的json字符串
                    flag, product_list_head, product_list_array = analysis_product_list(line)
                    if flag == True:
                        continue

                    ##解析数据行的商标名
                    brand_name = ','.join(line[3: product_list_head]).decode("utf-8")
                    if brand_name == u"图形":  #商标名是图形的其实是图形商标
                        continue
                    brand_name_china = brand.get_china_str(brand_name)
                    brand_name_pinyin = lazy_pinyin(brand_name_china)
                    brand_name_num, brand_name_eng = brand.get_not_china_list(brand_name)
                    brand_name_pinyin.extend(brand_name_eng)

                    ###数据行可用，开始逐个小项进行处理
                    #e_time = time.strftime("%Y%m%d%H%M%S", time.localtime())
                    class_no_set, product_no_set = get_product_no_list(product_list_array, item_dict)

                    similar_cnt = 0
                    last_class = {True:None, False:None}
                    for apply_date_his in record_time_dict.keys():
                        try:
                            if apply_date_his >= apply_date:
                                continue
                            #print "date %s has brand %d"%(apply_date_his, len(record_time_dict[apply_date_his]))
                            for class_no in class_no_set:
                                if class_no not in record_time_dict[apply_date_his]:
                                    continue
                                compare_list = record_time_dict[apply_date_his][class_no]
                                ###确定都有哪些拼音在大类中有
                                exsit_py = set(brand_name_pinyin).intersection(compare_list.keys())
                                py_low = compute_py_lowb(brand_name_pinyin)  ##根据长度确定确定排列组合的下界
                                # print u"共有拼音集合：%s, 下限长度 = %d"%(str(exsit_py), py_low)
                                if py_low == 0:
                                    continue
                                # 共有拼音排列组合
                                py_combi = combinations(exsit_py, py_low)
                                union = set()
                                for combi in py_combi:
                                    inter = set(record_time_dict[apply_date_his][class_no][combi[0]])
                                    s = combi[0]
                                    for i in range(1, len(combi)):
                                        inter = inter & record_time_dict[apply_date_his][class_no][combi[i]]
                                        s += " " + combi[i]

                                    union = union | inter
                                del compare_list
                                # print "py union has %d"%(len(union))
                                compare_list = list(union)
                                for i in range(len(compare_list)):
                                    compare_unit = record_id_dict[compare_list[i]]
                                    his_name = compare_unit["name"].decode("utf-8")
                                    his_name_pinyin = compare_unit["py"]
                                    his_name_china = compare_unit["ch"]
                                    brand_no_his = compare_unit["no"]
                                    last_class[class_no] = compare_unit
                                    if judge_pinyin(brand_name_pinyin, his_name_pinyin) == False:
                                        if len(brand_name_china) != len(his_name_china) or brand.glyphApproximation(
                                                brand_name_china, his_name_china) < 0.9:
                                            continue
                                    #计算相似度
                                    #print "brand %s, his%s, class %d"%(brand_name, his_name, class_no)
                                    similar, compare_Res = compute_similar(brand_name, his_name, gate)
                                    if similar == True:
                                        out_row = [brand_name.encode("gbk"), his_name.encode("gbk"), brand_no, brand_no_his,
                                                   class_no]
                                        out_row.extend(compare_Res)
                                        out_row.extend([brand_status, compare_unit["status"]])
                                        out_row.extend('1')
                                        writer.writerow(out_row)
                                        similar_cnt += 1
                                del compare_list
                        except:
                            pass
                            print traceback.format_exc()

                    if similar_cnt == 0 and brand_status == '1':
                        for last_class_key in last_class.keys():
                            compare_unit = last_class[last_class_key]
                            if compare_unit == None:
                                continue
                            similar_cnt += 1
                            his_name = compare_unit["name"].decode("utf-8")
                            brand_no_his = compare_unit["no"]
                            compare_Res = brand.getCharacteristics(brand_name, his_name)
                            out_row = [brand_name.encode("gbk"), his_name.encode("gbk"), brand_no, brand_no_his,
                                       i18n_type]
                            out_row.extend(compare_Res)
                            out_row.extend([brand_status, compare_unit["status"]])
                            out_row.extend('0')
                            writer.writerow(out_row)

                    if similar_cnt > 0 :
                        cnt_status[brand_status] += 1
                except Exception,e:
                    print traceback.format_exc()
                print "line = %d, 0 has %d, 1 has %d"%(line_cnt, cnt_status['0'], cnt_status['1'])
                if cnt_status['0'] >= (lowb + num) and cnt_status["1"] >= (lowb + num):
                    break
            del reader
            print "csv %s end"%file_name
        if cnt_status['0'] >= (lowb + num) and cnt_status["1"] >= (lowb + num):
            break
    f_out.close()

###计算拼音共同下界
def compute_py_lowb(brand_name_pinyin):
    b_list = brand_name_pinyin

    if len(b_list) < 3:
        # print b_list, h_list, cnt_comm
        return len(b_list)
    else:
        # print b_list,h_list
        return max(int(len(b_list) * 0.76), 2)

###判断两个商标中是否有同音字
def judge_pinyin(brand_name_pinyin, his_name_pinyin):
    b_list = brand_name_pinyin
    h_list = his_name_pinyin

    b_len = len(b_list)
    h_len = len(h_list)

    if h_len > len(b_list) + 2:  ##字数比较，被比较商标比原商标长2以上就pass
        return False

    cnt_comm = 0
    if b_len <= 3: ##商标长度小于等于3时，按乱序查找。即只要h串里有就行（可能重音，要标记）
        h_vis = form_vis_list(h_list)
        for i in range(b_len):
            for j in range(h_len):
                if h_vis[j] == False and h_list[j] == b_list[i]:
                    cnt_comm += 1
                    h_vis[j] = True
                    break
    if b_len > 3:  ##商标长度大于等于3时，按正序查找（就是算最长匹配距离）
        cnt_comm = brand.maxMatchLen(b_list, h_list)

    if b_len < 3 and cnt_comm == len(b_list) and h_len < b_len + 2:
        # print b_list, h_list, cnt_comm
        return True
    elif b_len >= 3 and cnt_comm >= max(int(max(b_len, h_len) * 0.76), 2):  #
        # print b_list,h_list
        return True

    return False

def form_vis_list(a_list):
    a_vis = []
    for i in range(len(a_list)):
        a_vis.append(False)
    return a_vis

###从redis中获取所有的历史商标构成的set(带时间戳的)
def getHistoryBrandWithTime(record_key_prefix, db, class_no_set = range(1, 46)):
    record_key_time_dict = {}
    record_id_dict = {}
    cnt_id = 0
    for class_no in class_no_set:
        ##依次获取每个大类的
        print "prepare class %d"%class_no
        record_key_time_set = db.smembers(record_key_prefix + str(class_no))
        for record_key in record_key_time_set:
            brand_name, brand_no, apply_time, brand_status = record_key.split("&*(")
            ##将商标名分解为中文、英文、数字，中文转拼音，英文分成词，并把拼音和英文词合并
            brand_china = brand.get_china_str(brand_name)
            brand_pinyin = lazy_pinyin(brand_china)
            brand_num, brand_eng = brand.get_not_china_list(brand_name)
            brand_pinyin.extend(brand_eng)
            record_id_dict[cnt_id] = {
                                        "name":brand_name,
                                        "no":brand_no,
                                        "status":brand_status,
                                        "py": brand_pinyin,
                                        "ch": brand_china
                                    }
            try:
                record_key_time_dict[apply_time]
            except:
                record_key_time_dict[apply_time] = {}

            try:
                record_key_time_dict[apply_time][class_no]
            except:
                record_key_time_dict[apply_time][class_no] = {}

            if len(brand_pinyin)==0:
                continue
            for pinyin in brand_pinyin:
                if record_key_time_dict[apply_time][class_no].has_key(pinyin) == False:
                    record_key_time_dict[apply_time][class_no][pinyin] = set()
                record_key_time_dict[apply_time][class_no][pinyin].add(cnt_id)
            cnt_id += 1
    return record_id_dict , record_key_time_dict

####两个商标计算相似度，按gate阈值过滤
def compute_similar(brand_name, his_name, gate):
    compare_Res = brand.getCharacteristics(brand_name, his_name)
    similar = False
    for index in range(len(compare_Res)):
        if gate[index] == 'C':
            if len(brand_name) < 4 and compare_Res[index] >= 0.76:
                similar = True
            elif len(brand_name) >= 4 and compare_Res[index] >= 0.76:
                similar = True
        elif gate[index] == 'N':
            continue
        else:
            if compare_Res[index] >= gate[index]:
                similar = True
    return similar,  compare_Res

###判断输入商标和历史商标是否有同小项：
def judgeSameClass(product_no_set, brand_no_his, rset_key_prefix, _pipe):
    pass
    for (class_no, product_no) in product_no_set:
        class_key = rset_key_prefix + brand_no_his + "::" + str(class_no)
        _pipe.sismember(class_key, product_no)
    judge_res = _pipe.execute()
    #print product_no_set
    #print judge_res
    if 1 in judge_res:
        #print "has 1"
        del judge_res[:]
        return True
    else:
        #print "no 1"
        del judge_res[:]
        return False




##975418个不同的商标，12277622
if __name__=="__main__":
    #form_train_data()
    try:
        lowb, num = int(sys.argv[1]), int(sys.argv[2])
    except:
        lowb , num = 0, 40000
    form_train_data_redis(lowb, num)
    #load_brand_item()






