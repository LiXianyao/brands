#-*-coding:utf8-*-#
import sys
import  ConfigParser
import redis
import os
from pypinyin import lazy_pinyin
from itertools import combinations
import time
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )
cf = ConfigParser.ConfigParser()
cf.read("redis.config")
fixed_ip = cf.get("redis","fixed_ip")
fixed_port = cf.get("redis","fixed_port")
fixed_db = cf.get("redis","fixed_db")
default_pwd = cf.get("redis","default_pwd")

from processdata.database import db_session
from similarity.brand import getCharacteristics, glyphApproximation, get_china_str
import csv
import time
import json
import  traceback

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

###redis数据库
def form_pre_data_redis(input_json):
    brand_name = input_json["name"]
    brand_name_china = get_china_str(brand_name)
    brand_name_pinyin = lazy_pinyin(brand_name_china)
    print "pinyin = %s"%(brand_name_pinyin)
    class_no_set = set(input_json["class"])
    print "brand name is %s, with searching class: %s"%(brand_name,str(class_no_set))

    #item_dict = load_brand_item()
    db = redis.StrictRedis(host=fixed_ip, port=fixed_port, db=fixed_db, password=default_pwd)
    _pipe = db.pipeline()

    # 中文编辑距离(越大越近)， 拼音编辑距离（越大越近）， 包含被包含（越大越近）
    # 排列组合（越大越近）， 中文含义近似（越大越近）， 中文字形近似（越大越近）
    # 英文编辑距离(越大越近)， 英文包含被包含（越大越近）， 英文排列组合（越大越近）
    # 数字完全匹配（越大越近）
    gate = ['C','C','C','C', 0.9, 0.8, 0.8, 'C', 'C',1.0]
    record_key = "rd::"
    record_key_time = "rdt::"
    rset_key_prefix = "rset::"
    detail_key_prefix = "dtl::"

    csv_name = u"pre_7_2_4.csv"
    title = []
    if os.path.exists(csv_name) == False:
        ###文件不存在，则把表头写一下
        title = [u"输入商标名", u"历史商标名", u"历史商标编号", u"所属大类", u"汉字编辑距离相似度", u"拼音相似度", u"汉字包含被包含",
                 u"汉字排列组合", u"汉字含义相近", u"汉字字形相似度", u"英文编辑距离相似度", u"英文包含被包含",
                 u"英文排列组合", u"数字完全匹配", u"历史商标状态", u"是否相似商标"]
        for i in range(len(title)):
            title[i] = title[i].encode("gbk")
    f_out = open(csv_name, "a")
    writer = csv.writer(f_out)
    if len(title) != 0:
        writer.writerow(title)


    ###获得所有的历史商标, 结果结构为 申请时间 -》 【商标名，商标编号，商标状态】
    record_id_dict, record_time_dict = getHistoryBrand(record_key, db, class_no_set)
    print "history brand ready"

    session = db_session()
    similar_cnt = {k:v for k,v in zip(class_no_set, [0]*len(class_no_set))}
    last_class = {k:v for k,v in zip(class_no_set, [None]*len(class_no_set))}
    s_time = time.strftime("%Y%m%d%H%M%S", time.localtime())
    try:
        for class_no in class_no_set:
            print class_no
            if class_no not in record_time_dict:
                continue
            compare_list = record_time_dict[class_no]
            ###确定都有哪些拼音在大类中有
            exsit_py = set(brand_name_pinyin).intersection(compare_list.keys())
            print exsit_py, brand_name_pinyin
            py_low = compute_py_lowb(brand_name_pinyin)##根据长度确定确定排列组合的下界
            #print u"共有拼音集合：%s, 下限长度 = %d"%(str(exsit_py), py_low)
            if py_low > 0:
                #共有拼音排列组合
                py_combi = combinations(exsit_py, py_low)
                union = set()
                for combi in py_combi:
                    inter = set(record_time_dict[class_no][combi[0]])
                    s = combi[0]
                    for i in range(1,len(combi)):
                        inter = inter & record_time_dict[class_no][combi[i]]
                        s += " " + combi[i]

                    union = union | inter
                    print "py combi %s has %d"%(s, len(inter))
                    #print "py union has %d"%(len(union))
            del compare_list
            print "py union has %d"%(len(union))
            compare_list = list(union)
            for i in range(len(compare_list)):
                compare_unit = record_id_dict[compare_list[i]]
                his_name = compare_unit["name"].decode("utf-8")
                brand_no_his = compare_unit["no"]
                last_class[class_no] = compare_unit
                if judge_pinyin(brand_name, his_name) == False:
                    if glyphApproximation(brand_name, his_name) < 0.9:
                        continue
                #计算相似度
                #print "brand %s, his%s, class %d"%(brand_name, his_name, class_no)
                similar, compare_Res = compute_similar(brand_name, his_name, gate)
                print his_name, brand_no_his
                print compare_Res
                if similar == True:
                    similar_cnt[class_no] += 1
                    out_row = [brand_name.encode("gbk"), his_name.encode("gbk"), brand_no_his,
                               class_no]
                    out_row.extend(compare_Res)
                    out_row.extend([compare_unit["status"]])
                    out_row.extend('1')
                    writer.writerow(out_row)
            del compare_list
    except:
        pass
        print traceback.format_exc()

    ###没有相似商标的情况
    for class_no in class_no_set:
        if similar_cnt[class_no] == 0:
            compare_unit = last_class[class_no]
            if compare_unit == None:
                continue
            similar_cnt[class_no] += 1
            his_name = compare_unit["name"].decode("utf-8")
            brand_no_his = compare_unit["no"]
            compare_Res = getCharacteristics(brand_name, his_name)
            out_row = [brand_name.encode("gbk"), his_name.encode("gbk"), brand_no_his,
                       class_no]
            out_row.extend(compare_Res)
            out_row.extend([compare_unit["status"]])
            out_row.extend('0')
            writer.writerow(out_row)
    e_time = time.strftime("%Y%m%d%H%M%S", time.localtime())
    print "start at %s, end at %s"%(s_time, e_time)
    f_out.close()


###判断两个商标中是否有同音
def judge_pinyin(brand_name_pinyin, his_name_pinyin):
    b_list = brand_name_pinyin
    h_list = his_name_pinyin
    h_vis = form_vis_list(h_list)

    cnt_comm = 0
    for i in range(len(b_list)):
        for j in range(len(h_list)):
            if h_vis[j] == False and h_list[j] == b_list[i]:
                cnt_comm += 1
                h_vis[j] = True
                break

    maxl = max(len(b_list), len(h_list))


    if len(b_list) < 3 and cnt_comm == len(b_list) and maxl <= len(b_list) * 3:
        #print b_list, h_list, cnt_comm
        return True
    elif len(brand_name_pinyin) >= 3 and cnt_comm >= max(int(maxl * 0.65), 2):#
        #print b_list,h_list
        return True

    return False


###计算拼音共同下界
def compute_py_lowb(brand_name_pinyin):
    b_list = brand_name_pinyin

    if len(b_list) < 3:
        # print b_list, h_list, cnt_comm
        return len(b_list)
    else:
        # print b_list,h_list
        return max(int(len(b_list) * 0.65), 2)

def form_vis_list(a_list):
    a_vis = []
    for i in range(len(a_list)):
        a_vis.append(False)
    return a_vis

###从redis中获取所有的历史商标构成的set(带时间戳的)
def getHistoryBrand(record_key_prefix, db, class_no_set):
    record_key_dict = {}
    record_id_dict = {}
    cnt_id = 0
    for class_no in class_no_set:
        ##依次获取每个大类的
        record_key_time_set = db.smembers(record_key_prefix + str(class_no))
        print "key %s has  %d"%(record_key_prefix + str(class_no), len(record_key_time_set))
        for record_key in record_key_time_set:
            brand_name, brand_no, brand_status = record_key.split("&*(")
            brand_china = get_china_str(brand_name)
            brand_pinyin = lazy_pinyin(brand_china)
            record_id_dict[cnt_id] = {
                                        "name":brand_name,
                                        "no":brand_no,
                                        "status":brand_status,
                                        "py": brand_pinyin
                                    }

            try:
                record_key_dict[class_no]
            except:
                record_key_dict[class_no] = {"ENG":set()}

            if len(brand_pinyin)==0:
                continue
                record_key_dict[class_no]["ENG"].add(cnt_id)
            for pinyin in brand_pinyin:
                if record_key_dict[class_no].has_key(pinyin) == False:
                    record_key_dict[class_no][pinyin] = set()
                record_key_dict[class_no][pinyin].add(cnt_id)
            cnt_id += 1
    return record_id_dict , record_key_dict

def compute_similar(brand_name, his_name, gate):
    compare_Res = getCharacteristics(brand_name, his_name)
    similar = False
    for index in range(len(compare_Res)):
        if gate[index] == 'C':
            if len(brand_name) < 4 and compare_Res[index] >= 0.65:
                similar = True
            elif len(brand_name) >= 4 and compare_Res[index] >= 0.7:
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
        json_Str = sys.argv[1]
        print json_Str
        input_json = json.loads(json_Str)
    except:
        print "argv[1] should be a json string!!"
        exit(0)
    form_pre_data_redis(input_json)
    #load_brand_item()






