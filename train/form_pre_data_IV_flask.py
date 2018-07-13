#-*-coding:utf8-*-#
import sys
import  ConfigParser
import redis
import os
from pypinyin import lazy_pinyin
from itertools import combinations
import datetime
import trans_pre_data
from similarity import brand
import threading
import  traceback
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )

###文件不存在，则把表头写一下
title_all = [u"输入商标名", u"历史商标名", u"历史商标编号", u"所属大类", u"汉字编辑距离相似度", u"拼音相似度", u"汉字包含被包含",
         u"汉字排列组合", u"汉字含义相近", u"汉字字形相似度", u"英文编辑距离相似度", u"英文包含被包含",
         u"英文排列组合", u"数字完全匹配", u"历史商标状态", u"是否相似商标"]
title_only = [u"输入商标名", u"历史商标名", u"历史商标编号", u"大类",u"是否相似商标"]

###redis数据库 的前缀
record_key = "rd::"
record_key_time = "rdt::"
rset_key_prefix = "rset::"
detail_key_prefix = "dtl::"


###获取商标的群组和小项名字、编号的映射关系
def load_brand_item():
    from processdata import brand_item
    item_list = brand_item.BrandItem.query.all()
    #brand_item.db_session.rollback()
    item_dict = {}
    for item in item_list:
        item_name = item.item_name
        item_no = item.item_no
        item_dict[item_no] = item_name
    del brand_item
    return item_dict


###redis数据库
def form_pre_data_flask(input_json, record_id_dict, record_key_dict, item_dict, AllClass = True, AllRes = True):
    brand_name = input_json["name"]
    brand_name_china = brand.get_china_str(brand_name)
    brand_name_pinyin = lazy_pinyin(brand_name_china)
    brand_name_num , brand_name_eng = brand.get_not_china_list(brand_name)
    brand_name_pinyin.extend(brand_name_eng)
    #print "pinyin plus eng = %s"%(brand_name_pinyin)
    #print AllClass, AllRes
    if AllClass == False:
        class_no_set = input_json["class"]
    else:
        class_no_set = range(1,46)
    reload(brand)
    print "brand name is %s, with searching class: %s, len id_dict=%d, len key_dict=%d"%(brand_name,str(class_no_set), len(record_id_dict),len(record_key_dict))

    # 中文编辑距离(越大越近)， 拼音编辑距离（越大越近）， 包含被包含（越大越近）
    # 排列组合（越大越近）， 中文含义近似（越大越近）， 中文字形近似（越大越近）
    # 英文编辑距离(越大越近)， 英文包含被包含（越大越近）， 英文排列组合（越大越近）
    # 数字完全匹配（越大越近）
    gate = ['C','C','C','C', 'N', 0.8, 0.8, 'C', 'C',1.0]

    similar_cnt = {k:v for k,v in zip(class_no_set, [0]*len(class_no_set))}
    last_class = {k:v for k,v in zip(class_no_set, [None]*len(class_no_set))}
    start_time_c = datetime.datetime.now()

    return_list = []
    try:
        for class_no in class_no_set:
            #print class_no
            if class_no >= len(record_key_dict):
                continue
            #print "in ",class_no
            compare_list = record_key_dict[class_no]
            ###确定都有哪些拼音在大类中有
            exsit_py = set(brand_name_pinyin).intersection(compare_list.keys())
            #print brand_name_pinyin, exsit_py
            #print exsit_py, brand_name_pinyin
            py_low = compute_py_lowb(brand_name_pinyin)##根据长度确定确定排列组合的下界
            #print u"共有拼音集合：%s, 下限长度 = %d"%(str(exsit_py), py_low)
            if py_low > 0:
                #共有拼音排列组合
                py_combi = combinations(exsit_py, py_low)
                union = set()
                for combi in py_combi:
                    inter = set(record_key_dict[class_no][combi[0]])
                    s = combi[0]
                    for i in range(1,len(combi)):
                        inter = inter & record_key_dict[class_no][combi[i]]
                        s += " " + combi[i]

                    union = union | inter
                    #print "py combi %s has %d"%(s, len(inter))
                    #print "py union has %d"%(len(union))
            else:
                continue
            del compare_list
            #print "py union has %d"%(len(union))
            compare_list = list(union)
            for i in range(len(compare_list)):
                compare_unit = record_id_dict[class_no][compare_list[i]]
                his_name = compare_unit[0].decode("utf-8")
                brand_no_his = compare_unit[1]
                his_name_pinyin = compare_unit[3]
                his_name_china = compare_unit[4]
                last_class[class_no] = compare_unit
                #start_time_s = datetime.datetime.now()
                #print brand_name, his_name
                if judge_pinyin(brand_name_pinyin, his_name_pinyin) == False:
                    if len(brand_name_china) != len(his_name_china) or brand.glyphApproximation(brand_name_china, his_name_china) < 0.9:
                        continue
                #end_time_s = datetime.datetime.now()
                #cost_time_s = (end_time_s - start_time_s).total_seconds()
                #print "两商标计算拼音重合量的时间消耗为：", cost_time_s  #通常在1.5ms

                #start_time_c = datetime.datetime.now()
                similar, compare_Res = compute_similar(brand_name, his_name, gate)
                #print brand_name, his_name, compare_Res
                #end_time_c = datetime.datetime.now()
                #cost_time_c = (end_time_c - start_time_c).total_seconds()
                #print "两商标计算十种特征值的时间消耗为：", cost_time_c  ##通常在 100~ 150ms，取决于数据，也有2ms就算完的情况
                if similar == True:
                    similar_cnt[class_no] += 1 ###构造返回结果：近似商标名（及特征）
                    if (AllRes == True):
                        out_row = [brand_name, his_name, brand_no_his,
                                   class_no]
                        out_row.extend(compare_Res)
                        out_row.extend([compare_unit[2]])
                        out_row.extend('1')
                        return_list.append(out_row)
                    else:
                        out_row = [brand_name, his_name, brand_no_his, class_no, True]
                        return_list.append(out_row)
            del compare_list
    except:
        pass
        print traceback.format_exc()

    ###没有相似商标的情况
    try:
        for class_no in class_no_set:
            #continue
            if similar_cnt[class_no] == 0:
                compare_unit = last_class[class_no]
                if compare_unit == None:
                    continue
                similar_cnt[class_no] += 1
                his_name = compare_unit[0].decode("utf-8")
                brand_no_his = compare_unit[1]
                compare_Res = brand.getCharacteristics(brand_name, his_name)###构造返回结果：近似商标名（及特征）
                if AllRes == True:
                    out_row = [brand_name, his_name, brand_no_his,
                               class_no]
                    out_row.extend(compare_Res)
                    out_row.extend([compare_unit[3]])
                    out_row.extend('0')
                    return_list.append(out_row)
                else:
                    out_row = [brand_name, his_name, brand_no_his, class_no, False]
                    return_list.append(out_row)
    except:
        print traceback.format_exc()
    #print "reload!!"

    if AllRes == True:
        try:
            reload(trans_pre_data)
            db, _pipe = createRedisConnection()
            itemList = getItemListOfBrand(return_list, item_dict, _pipe)
            return_list = trans_pre_data.trans_pre_data_web(return_list, itemList, class_no_set)
        except:
            print traceback.format_exc()
    else:
        return_list = {u"近似名字组": return_list}
        try:
            return_list = bind_title(return_list, AllRes)
        except:
            print traceback.format_exc()

    ###调用预测模块添东西
    end_time_c = datetime.datetime.now()
    cost_time_c = (end_time_c - start_time_c).total_seconds()
    print u"进程查询耗时为 :", cost_time_c
    return return_list

##根据给定的商标id和大类。获取这个商标在这个大类下的小项id
def getItemListOfBrand(data_list , item_dict, _pipe):
    for brand in data_list:
        brand_no = brand[2]
        class_no = brand[3]
        rset_key = rset_key_prefix + brand_no + "::" + str(class_no)
        #print rset_key
        _pipe.smembers(rset_key)
    itemList = _pipe.execute()
    for i in range(len(data_list)):
        #print itemList[i]
        trans_itemList_i = []
        for item_no in itemList[i]:
            trans_itemList_i.append(item_dict[item_no])
        #print trans_itemList_i
        itemList[i] = trans_itemList_i
    return itemList


###为近似名字组结果绑定键值
def bind_title(return_list, AllRes):
    if AllRes == True:
        for i in range(len(return_list)):
            return_list[i] = dict(zip(title_all, return_list[i]))
    else:
        for i in range(len(return_list)):
            return_list[i] = dict(zip(title_only, return_list[i]))
    return return_list

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


###计算拼音共同下界
def compute_py_lowb(brand_name_pinyin):
    b_list = brand_name_pinyin

    if len(b_list) < 3:
        # print b_list, h_list, cnt_comm
        return len(b_list)
    else:
        # print b_list,h_list
        return max(int(len(b_list) * 0.76), 2)

def form_vis_list(a_list):
    a_vis = []
    for i in range(len(a_list)):
        a_vis.append(False)
    return a_vis

###从redis中获取所有的历史商标构成的set(带时间戳的)
def getHistoryBrand(record_key_prefix, db, class_no_set):
    record_key_dict = []
    record_id_dict = []
    for i in range(46):
        record_key_dict.append({})
        record_id_dict.append([])
    for class_no in class_no_set:
        ##依次获取每个大类的
        formOneClassData(record_key_prefix, class_no, db, record_id_dict, record_key_dict)

    return record_id_dict , record_key_dict

###读取单个大类的数据并构造拼音字集合
def formOneClassData(record_key_prefix, class_no, db, record_id_dict, record_key_dict):
    record_key_time_set = db.smembers(record_key_prefix + str(class_no))
    cnt_id = 0
    print "prepare key %s" % (record_key_prefix + str(class_no))
    for record_key in record_key_time_set:
        brand_name, brand_no, brand_status = record_key.split("&*(")

        ##将商标名分解为中文、英文、数字，中文转拼音，英文分成词，并把拼音和英文词合并
        brand_china = brand.get_china_str(brand_name)
        brand_pinyin = lazy_pinyin(brand_china)
        brand_num, brand_eng = brand.get_not_china_list(brand_name)
        brand_pinyin.extend(brand_eng)

        record_id_dict[class_no].append([
            brand_name,
            brand_no,
            brand_status,
            brand_pinyin,
            brand_china
        ])
        cnt_id += 1
        if len(brand_pinyin) == 0:
            continue
        for pinyin in brand_pinyin:
            if record_key_dict[class_no].has_key(pinyin) == False:
                record_key_dict[class_no][pinyin] = set()
            record_key_dict[class_no][pinyin].add(cnt_id - 1)

    print "key %s is ready" % (record_key_prefix + str(class_no))
    record_key_time_set.clear()
    del record_key_time_set
    """
    if "ai" in record_key_dict[class_no].keys():
        printcnt = 0
        for id in record_key_dict[class_no]["ai"]:
            print str(record_id_dict[class_no][id]).replace('u\'', '\'').decode("unicode-escape")
            printcnt += 1
            if printcnt >= 5:
                break
    """

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

##读取配置并创建redis连连接
def createRedisConnection():
    cf = ConfigParser.ConfigParser()
    cf.read("redis.config")
    redis_ip = cf.get("redis", "redis_ip")
    redis_port = cf.get("redis", "redis_port")
    redis_db = cf.get("redis", "redis_db")
    redis_pwd = cf.get("redis", "redis_pwd")

    db = redis.StrictRedis(host=redis_ip, port=redis_port, db=redis_db, password=redis_pwd)
    _pipe = db.pipeline()
    return db, _pipe
##975418个不同的商标，12277622
if __name__=="__main__":
    #form_train_data()
    """
    try:
        json_Str = sys.argv[1]
        print json_Str
        input_json = json.loads(json_Str)
    except:
        print "argv[1] should be a json string!!"
        exit(0)


    class_no_set = set(input_json["class"])

    db, _pipe = createRedisConnection()
    ###获得所有的历史商标, 结果结构为 申请时间 -》 【商标名，商标编号，商标状态】
    record_id_dict, record_key_dict = getHistoryBrand(record_key, db, class_no_set)
    print "history brand ready"

    return_list = form_pre_data_flask(input_json, record_id_dict, record_key_dict)
    #load_brand_item()

    csv_name = u"pre_7_2_flask.csv"
    if os.path.exists(csv_name) == False:
        for i in range(len(title_all)):
            title_all[i] = title_all[i].encode("utf-8")
    f_out = open(csv_name, "a")
    writer = csv.writer(f_out)
    if len(title_all) != 0:
        writer.writerow(title_all)

    for row in return_list:
        writer.writerow(row)
    f_out.close()
    """






