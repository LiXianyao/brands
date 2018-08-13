#-*-coding:utf8-*-#
from brand_item import BrandItem
import csv
import time
import json
import  traceback
import redis
import ConfigParser
import sys
from pypinyin import  lazy_pinyin, Style
from itertools import combinations
reload(sys)
sys.setdefaultencoding( "utf-8" )
sys.path.append('..')

from similarity import brand

cf = ConfigParser.ConfigParser()
cf.read("redis.config")
redis_ip = cf.get("redis","redis_ip")
redis_port = cf.get("redis","redis_port")
redis_db = cf.get("redis","redis_db")
redis_pwd = cf.get("redis","redis_pwd")

date_origi_format = "%Y年%m月%d日"
date_target_format = "%Y%m%d"

rank_key_prefix = "brank::"
data_key_prefix = "bData::"
pyset_key_prefix = "bPySet::"  # set
item_key_prefix = "bItem::"

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

def clear_redis_key(prefix, db, _pipe):
    old_data = db.keys(prefix + "*")
    for key in old_data:
        _pipe.delete(key)
    _pipe.execute()
    print "delete keys %s, %d"%(prefix, len(old_data))
    del old_data[:]

####record表存到redis中
def form_brand_record_redis():
    row_len = 9
    file_names = range(1, 14)
    batch = 200000

    item_dict = load_brand_item()
    old = 0
    db = redis.StrictRedis(host=redis_ip, port=redis_port, db='1', password=redis_pwd)
    _pipe = db.pipeline()

    id_name_set = [{}]
    id_name_set_size = {}
    for i in range(1,46):
        id_name_set.append({})
        id_name_set_size[i] = 0

    clear_redis_key(data_key_prefix, db, _pipe)
    clear_redis_key(item_key_prefix, db, _pipe)
    clear_redis_key(pyset_key_prefix, db, _pipe)
    clear_redis_key(rank_key_prefix, db, _pipe)

    cnt_op = 0
    term = 4
    for file_name in file_names:
        file_name = str(file_name) + ".csv"
        with open("../../csv3/" + file_name, "rU") as csv_file:
            reader = csv.reader(csv_file, delimiter=',', quotechar='\n')
            line_cnt = 0
            ok_cnt = 0
            for line in reader:
                line_cnt += 1
                if line_cnt == 1:
                    continue
                if len(line) < row_len:
                    #print "line %d error, as:" % (line_cnt)
                    continue

                ###解析csv字段，并确定数据行的可用性
                try:
                    ###解析数据行尾部
                    brand_no, apply_date, i18n_type, brand_status = analysis_row_end(line)
                    if len(brand_status) > 1:
                        continue

                    ###解析小项列表的json字符串
                    flag, product_list_head, product_list_array = analysis_product_list(line)
                    if flag == True:
                        continue

                    ##解析数据行的商标名
                    brand_name = ','.join(line[3: product_list_head])
                    brand_name = brand_name.strip()
                    if brand_name == u"图形" or len(brand_name) == 0:  # 商标名是图形的其实是图形商标
                        continue

                    ###数据行可用，开始逐个小项进行处理
                    class_no_set, product_no_set = get_product_no_list(product_list_array, item_dict)
                    ##用商标名+id，按大类聚合
                    ##检查大类里是否已经有了这个id+商标名组合
                    bkey = brand_no + "&*(" + brand_name
                    #print brand_name
                    #print product_no_set
                    for (class_no, product_no) in product_no_set:
                        try:
                            ###如果已经在集合里，只可能需要更新这个商标的小项记录
                            b_setid = id_name_set[class_no][bkey]
                        except:
                            ##否则，需要将他先加入某个大类，分配一个id，然后还要存储它的数据、构造读音集合等
                            id_name_set[class_no][bkey] = id_name_set_size[class_no]
                            b_setid = id_name_set[class_no][bkey]
                            id_name_set_size[class_no] += 1
                            _pipe.zadd(rank_key_prefix + str(class_no) , b_setid, bkey)
                            cnt_op += 1
                            cnt_op += add_new_brand(brand_name, brand_no, brand_status, apply_date, class_no, b_setid, _pipe)

                        _pipe.sadd(item_key_prefix + str(class_no) + "::" + str(b_setid), product_no)
                        cnt_op += 1
                    ok_cnt += 1
                except Exception, e:
                    pass
                    print traceback.format_exc()

                if cnt_op >batch and line_cnt >= old:
                    try:
                        cnt_op = 0
                        _pipe.execute()
                    except:
                        print "csv_file " + file_name + " produce %d rows" % (line_cnt)
                        print "error!!!", traceback.format_exc()

                if line_cnt > term:
                    pass
                    #break
                #break
            try:
                cnt_op = 0
                _pipe.execute()
            except:
                print "error!!!", traceback.format_exc()
            print "csv_file " + file_name + " has %d rows, legal rows are %d" % (line_cnt, ok_cnt)
            if line_cnt > term:
                pass
                #break

    for class_no in range(1, 46):
        record_key = rank_key_prefix + str(class_no)
        # print record_key
        set_size = db.zcard(record_key)

        data_key = data_key_prefix + str(class_no) + "::*"
        # print data_key
        data_key_set = db.keys(data_key)
        set_data_size = len(data_key_set)

        item_key = item_key_prefix + str(class_no) + "::*"
        # print item_key
        item_key_set = db.keys(item_key)
        set_item_size = len(item_key_set)
        print "key %s has %d keys, while key %s has %d" % (record_key, set_size, item_key, set_item_size)

###在redis数据库中增加一个新商标的相关数据
def add_new_brand(brand_name, brand_no, brand_status, apply_date, class_no, b_setid, _pipe):
    ##将商标名分解为中文、英文、数字，中文转拼音，英文分成词，并把拼音和英文词合并
    cnt_op = 0
    brand_china = brand.get_china_str(brand_name)
    brand_pinyin = lazy_pinyin(brand_china, style=Style.TONE3)
    brand_num, brand_eng = brand.get_not_china_list(brand_name)

    record_dict = {
        "bid": b_setid,
        "name": brand_name,
        "no": brand_no,
        "sts": brand_status,
        "py" : ','.join(brand_pinyin),
        "ch" : brand_china,
        "eng": ','.join(brand_eng),
        "num": ','.join(brand_num),
        "date": apply_date
    }
    ###存储数据
    data_key = data_key_prefix + str(class_no) + "::" + str(b_setid)
    #print data_key
    #print record_dict
    _pipe.hmset(data_key, record_dict)
    cnt_op += 1


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
                set_key = pyset_key_prefix + str(class_no) + "::" + ','.join(combi) ##key = "bPySet::1::ni2,hao3" ,类似这样的
                #print set_key
                _pipe.sadd(set_key, b_setid)
                cnt_op += 1
    else:
        print u"还有这样？？brand %s 解析出来什么都没有"%(brand_name)
    return cnt_op


##975418个不同的商标，12277622
if __name__=="__main__":
    form_brand_record_redis()
    #load_brand_item()






