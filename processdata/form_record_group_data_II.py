#-*-coding:utf8-*-#
from brand_item import BrandItem
import csv
import time
import json
import  traceback
import redis
import ConfigParser
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

cf = ConfigParser.ConfigParser()
cf.read("redis.config")
fixed_ip = cf.get("redis","fixed_ip")
fixed_port = cf.get("redis","fixed_port")
fixed_db = cf.get("redis","fixed_db")
default_pwd = cf.get("redis","default_pwd")

date_origi_format = "%Y年%m月%d日"
date_target_format = "%Y%m%d"

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

####record表存到redis中
def form_brand_record_redis():
    row_len = 9
    file_names = range(5, 6)
    batch = 5000

    item_dict = load_brand_item()
    old = 0
    db = redis.StrictRedis(host=fixed_ip, port=fixed_port, db=fixed_db, password=default_pwd)
    _pipe = db.pipeline()
    """
    old_data = db.keys("rdt::*")
    for key in old_data:
        _pipe.delete(key)
    _pipe.execute()
    print "delete keys %d"%(len(old_data))
    del old_data[:]
    """


    record_key_prefix = "rd::"
    record_key_time_prefix = "rdt::" #set
    rset_key_prefix = "rset::"
    detail_key_prefix = "dtl::"


    for file_name in file_names:
        file_name = str(file_name) + ".csv"
        with open("/root/csv3/" + file_name, "rU") as csv_file:
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
                    if brand_name == "图形":  # 商标名是图形的其实是图形商标
                        continue

                    ###数据行可用，开始逐个小项进行处理
                    class_no_set, product_no_set = get_product_no_list(product_list_array, item_dict)
                    ##商标名+id表(按大类聚合)
                    for class_no in class_no_set:
                        record_key = record_key_prefix + str(class_no)
                        _pipe.sadd(record_key, brand_name + "&*(" + brand_no + "&*(" + brand_status)

                        record_time_key = record_key_time_prefix + str(class_no)
                        _pipe.sadd(record_time_key, brand_name + "&*(" + brand_no + "&*(" + apply_date + "&*(" + brand_status)
                    ###商标具体内容表（暂缺）
                    detail_key = detail_key_prefix + brand_no
                    #_pipe.hmset(detail_key, {})
                    #print record_key, record_time_key

                    for (class_no, product_no) in product_no_set:
                        pass
                        #rset_key = rset_key_prefix + brand_no + "::" + str(class_no)
                        #_pipe.sadd(rset_key, product_no)
                        #print rset_key
                    ok_cnt += 1
                except Exception, e:
                    pass
                    #print traceback.format_exc()

                if line_cnt % batch == 0 and line_cnt >= old:
                    try:
                        _pipe.execute()
                    except:
                        print "csv_file " + file_name + " produce %d rows" % (line_cnt)
                        print "error!!!", traceback.format_exc()
                        # break
                #break
            for class_no in range(1,46):
                record_key = record_key_prefix + str(class_no)
                record_time_key = record_key_time_prefix + str(class_no)
                set_size = db.scard(record_key)
                set_time_size = db.scard(record_time_key)
                print "key %s has %d keys, while key %s has %d"%(record_key, set_size, record_time_key, set_time_size)
            try:
                _pipe.execute()
            except:
                print "error!!!", traceback.format_exc()
            print "csv_file " + file_name + " has %d rows, legal rows are %d" % (line_cnt, ok_cnt)

##975418个不同的商标，12277622
if __name__=="__main__":
    form_brand_record_redis()
    #load_brand_item()






