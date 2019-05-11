#-*-coding:utf8-*-#
import sys
import  ConfigParser
import redis
import os
from pypinyin import lazy_pinyin, Style
from itertools import combinations
import getopt
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )
cf = ConfigParser.ConfigParser()
cf.read("redis.config")
redis_ip = cf.get("redis","redis_ip")
redis_port = cf.get("redis","redis_port")
redis_db = cf.get("redis","redis_db")
redis_pwd = cf.get("redis","redis_pwd")

u"""
此脚本已成废案，预计过个把月删掉
"""

from processdata.brand_item import BrandItem
from similarity import brand
import csv
import time
import json
import  traceback

###日期格式转换字符串
date_origi_format = u"%Y年%m月%d日"
date_target_format = "%Y%m%d"

###redis数据库 的前缀
data_key_prefix = "bData::"
pyset_key_prefix = "bPySet::"  # set
item_key_prefix = "bItem::"

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
def form_train_data_redis(lowb, num, csv_name, limit_date, taskid):
    row_len = 9
    file_names = range(1, 14)##其实就是从a.csv到b.csv里面获取数据

    item_dict = load_brand_item()
    db = redis.StrictRedis(host=redis_ip, port=redis_port, db=redis_db, password=redis_pwd)
    _pipe = db.pipeline()

    # 筛选近似商标时，对每个特征取值的门限
    # 中文编辑距离(越大越近)， 拼音编辑距离（越大越近）， 包含被包含（越大越近）
    # 排列组合（越大越近）， 中文含义近似（越大越近）， 中文字形近似（越大越近）
    # 英文编辑距离(越大越近)， 英文包含被包含（越大越近）， 英文排列组合（越大越近）
    # 数字完全匹配（越大越近）
    gate = ['C', 0.85, 'C', 'C', 'N', 0.9, 0.8, 'C', 'C',1.0]

    ###保存近似数据的csv文件构造
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

    cnt_status = {'0':0, "1":0}
    for file_name in file_names:
        file_name = str(file_name) + ".csv"
        with open("../../csv3/" + file_name, "rU") as csv_file:
            reader = csv.reader(csv_file, delimiter=',', quotechar='\n')
            line_cnt = 0
            for line in reader:
                line_cnt += 1
                if line_cnt == 1: #跳过表头
                    continue
                if len(line) < row_len:#跳过字段不足的数据（源于csv单元格中出现了换行符）
                    #print "line %d error, as:"%(line_cnt)
                    continue

                ###解析csv字段，并确定数据行的可用性
                try:
                    ###解析数据行尾部
                    brand_no, apply_date, i18n_type, brand_status = analysis_row_end(line)
                    if apply_date < limit_date: ##申请时间小于执行设定值，过滤
                        continue
                    if len(brand_status) != 1: ##字段取值异常，过滤
                        continue
                    if cnt_status[brand_status] < lowb or cnt_status[brand_status] >= (lowb + num): ##对当前取的0/1商标已满足目标，过滤
                        continue

                    ###解析小项列表的json字符串
                    flag, product_list_head, product_list_array = analysis_product_list(line)
                    if flag == True:
                        continue

                    ##解析数据行的商标名
                    brand_name = ','.join(line[3: product_list_head]).decode("utf-8")
                    brand_name = brand_name.strip()
                    if brand_name == u"图形" and len(brand_name) > 0:  #商标名是图形的其实是图形商标/表格有问题，数据异常，过滤
                        continue

                    brand_name_china = brand.get_china_str(brand_name)
                    brand_name_pinyin = lazy_pinyin(brand_name_china, style=Style.TONE3)
                    brand_name_num, brand_name_eng = brand.get_not_china_list(brand_name)
                    brand_name_pinyin.extend(brand_name_eng)

                    ###数据行可用，开始逐个小项进行处理
                    class_no_set, product_no_set = get_product_no_list(product_list_array, item_dict)

                    similar_cnt = 0
                    last_class = {}
                    py_low = compute_py_lowb(brand_name_pinyin)  ##根据长度确定确定排列组合的下界
                    py_combi = combinations(brand_name_pinyin, py_low)
                    try:
                        for class_no in class_no_set:
                            if py_low > 0:
                                # 共有拼音排列组合
                                union = set()
                                for combi in py_combi:
                                    if len(combi) == 1:
                                        inter = db.smembers(pyset_key_prefix + str(class_no) + "::" + combi[0])
                                        # s = combi[0]
                                    else:
                                        ###多元组，将redis中多个集合合并
                                        inter, s = get_pycombi(db, combi, class_no)
                                    union = union | inter
                                    # print "class = %d,py combi %s has %d"%(class_no, s, len(inter))
                            else:  ###没有汉字没有英文没有数字
                                continue
                            compare_list = get_union_data(_pipe, class_no, union)
                            for i in range(len(compare_list)):
                                compare_unit = compare_list[i]
                                his_apply_date = compare_unit["date"]
                                if his_apply_date >= apply_date:  ##只看比当前选定商标申请时间早的商标
                                    continue

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
                                               class_no, compare_Res, brand_status, compare_unit["sts"], '1']
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
                                       last_class_key, compare_Res, brand_status, compare_unit["sts"], "0"]
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
    from trans_train_data_mysql import init_trans
    init_trans(taskId=taskid)  ##调用文件，将csv数据转化

###计算拼音共同下界
def compute_py_lowb(brand_name_pinyin):
    b_list = brand_name_pinyin

    if len(b_list) < 3:
        # print b_list, h_list, cnt_comm
        return len(b_list)
    else:
        # print b_list,h_list
        return max(int(len(b_list) * 0.67), 2)

###判断两个商标中是否有同音字
def judge_pinyin(brand_name_pinyin, his_name_pinyin):
    b_list = brand_name_pinyin
    h_list = his_name_pinyin.split(",")
    b_len = len(b_list)
    h_len = len(h_list)

    cnt_comm = 0
    if b_len <= 3:  ##商标长度小于等于3时，按乱序查找。即只要h串里有就行（可能重音，要标记）
        h_vis = [False] * (h_len)
        for i in range(b_len):
            for j in range(h_len):
                # print h_list[j], b_list[i], h_list[j] == b_list[i]
                if h_vis[j] == False and h_list[j] == b_list[i]:
                    cnt_comm += 1
                    h_vis[j] = True
                    break
    if b_len > 3:  ##商标长度大于等于3时，按正序查找（就是算最长匹配距离）
        cnt_comm = brand.maxMatchLen(b_list, h_list)

    # print "py check ===> ", b_list, h_list, cnt_comm
    if h_len > cnt_comm + 4:  ##字数比较，被比较商标与输入商标，在公有部分的基础上长4以上就pass
        return False

    if b_len < 3 and cnt_comm == len(b_list):
        # 输入商标的长度只有1或者2， 那么共有部分必须是1或者2
        return True
    elif b_len >= 3 and cnt_comm >= max(int(b_len * 0.67), 2):  #
        # 输入商标长度为3或者以上，那么部分重合就可以
        return True

    return False

##根据编号集合和大类号，获取对应的数据
def get_union_data(_pipe, class_no, union):
    for bid in union:
        bdata_key = data_key_prefix + str(class_no) + "::" + str(bid)
        _pipe.hgetall(bdata_key)
    return _pipe.execute()


####从redis中读取读音组合对应的商标号集合
##对于2元组，直接取，大于二元组的，用redis去算交集
def get_pycombi(db, combi, class_no):
    inter_args = []
    combi_len = len(combi)
    first_key = pyset_key_prefix + str(class_no) + "::" + ','.join([combi[0], combi[1]])
    combi_str= combi[0] + "," + combi[1]

    for i in range(1, combi_len/2):
        set_key = pyset_key_prefix + str(class_no) + "::" + ','.join([combi[i * 2], combi[i * 2 + 1]])
        combi_str+= combi[i * 2] + "," + combi[i * 2 + 1]
        inter_args.append(set_key)

    if combi_len % 2 == 1:##奇数个读音
        set_key = pyset_key_prefix + str(class_no) + "::" + ','.join([combi[0] , combi[-1]])
        combi_str+= combi[-1]
        inter_args.append(set_key)

    inter = db.sinter(first_key, *tuple(inter_args))
    return inter, combi_str

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


"""脚本的命令行输入提示"""
def printUsage():
    print "usage: form_train_data_IV.py [-i <taskid>] -l <lowerbound> -n <row num> -t <applicate time>"
    print u"-i <taskid> 可选参数，参数内容是任务标识，将作为训练数据提取结果、中间文件、结果模型等一系列产生的文件名的一部分，用于区分。默认为当前时间戳"
    print u"-l <lowerbound>  -n <row num>,可选参数，分别是取数下限和使用行数。比如-l 0 -n 2000, 则是对标记为1和0的商标名，分别从遇到的第0个这样的合法（解析csv无错）商标开始，取2000。" \
          u"最后应有 2*n个输入商标，为0数和为1数一样多（也有可能实际就无法取够），每个输入商标至少有一行数据，对应一个和这个输入商标相似的商标。"
    print u"-t <applicate time> 可选参数，用来做输入商标的注册时间下限，比如若填了2015,则只有2015年1月起的数据会被作为输入商标。（字典序，201501 < 20150101，所以要锁定范围的话尽量填的长）"

def check_file_exist(fdir, file_name):
    now_name = file_name
    suffiex = 1
    if fdir[-1] != u'/':
        fdir += u'/'
    while os.path.exists(fdir + now_name) == True:
        now_name = file_name.split(".")[0] + "(" + str(suffiex) + ")" + file_name.split(".")[-1]
        suffiex += 1
    return now_name

##975418个不同的商标，12277622
if __name__=="__main__":
    #form_train_data()

    try:
        opts, args = getopt.getopt(sys.argv[1:],"l:i:n:t:",["lowerbound=","id=","num=","time="])
    except getopt.GetoptError:
        #参数错误
        printUsage()
        sys.exit(-1)
    print opts

    nowtime = time.strftime("%Y%m%d%H", time.localtime())##时间戳
    fdir = u"data/origin/"
    lowb, num, limit_date = 0, 10, "2015"
    ####以上为命令行参数的默认值


    for opt,arg in opts:
        if opt in ("-l","--lowerbound"):
            lowb = arg
        elif opt in ("-i","--id"):
            nowtime = arg
        elif opt in ("-n", "--num"):
            num = arg
        elif opt in ("-t", "--time"):
            limit_date = arg

    filename = u"train_" + nowtime + u".csv"
    filename = check_file_exist(fdir, filename)

    form_train_data_redis(lowb, num, filename, limit_date, taskid=nowtime)
    #load_brand_item()






