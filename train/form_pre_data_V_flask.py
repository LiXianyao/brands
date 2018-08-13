#-*-coding:utf8-*-#
import sys
import  ConfigParser
import redis
import os
from pypinyin import lazy_pinyin, Style
from itertools import combinations
import datetime
import trans_pre_data
from similarity import brand
import  traceback
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )

###redis数据库 的前缀
rank_key_prefix = "brank::"
data_key_prefix = "bData::"
pyset_key_prefix = "bPySet::"  # set
item_key_prefix = "bItem::"


###获取商标的群组和小项名字、编号的映射关系
def load_brand_item():
    from processdata import brand_item
    item_list = brand_item.BrandItem.query.all()
    #brand_item.db_session.rollback()
    item_dict = {}
    for item in item_list:
        item_name = item.item_name
        item_no = item.item_no
        class_no = int(item.class_no)
        if item_dict.has_key(class_no) == False:
            item_dict[class_no] = {}
        item_dict[class_no][item_no] = (item_no, item_name)
    del brand_item
    return item_dict


###redis数据库
def form_pre_data_flask(input_json, item_dict, db, _pipe, logger):
    brand_name = input_json["name"]
    brand_name_china = brand.get_china_str(brand_name)
    brand_name_pinyin = lazy_pinyin(brand_name_china, style=Style.TONE3)
    brand_name_num , brand_name_eng = brand.get_not_china_list(brand_name)
    brand_name_pinyin.extend(brand_name_eng)
    class_no_set = input_json["categories"]
    logger.debug("brand name is %s, with searching class: %s"%(brand_name,str(class_no_set)))
    error_occur = False ###标记运行期间是否发生错误

    # 中文编辑距离(越大越近)， 拼音编辑距离（越大越近）， 包含被包含（越大越近）
    # 排列组合（越大越近）， 中文含义近似（越大越近）， 中文字形近似（越大越近）
    # 英文编辑距离(越大越近)， 英文包含被包含（越大越近）， 英文排列组合（越大越近）
    # 数字完全匹配（越大越近）
    gate = ['C',0.8,'C','C', 'N', 0.67, 0.67, 'C', 'C',1.0]

    similar_cnt = {k:v for k,v in zip(class_no_set, [0]*len(class_no_set))}  ##累计每个类别找到的近似商标数
    last_class = {k:v for k,v in zip(class_no_set, [None]*len(class_no_set))}  ##保存每个类别的近似商标
    start_time_c = datetime.datetime.now()

    return_list = []
    py_low = compute_py_lowb(brand_name_pinyin)##根据长度确定确定排列组合的下界
    py_combi = combinations(brand_name_pinyin, py_low)
    try:
        for class_no in class_no_set:
            if py_low > 0:
                #共有拼音排列组合
                union = set()
                for combi in py_combi:
                    if len(combi)  == 1:
                        inter = db.smembers(pyset_key_prefix + str(class_no) + "::" + combi[0])
                        #s = combi[0]
                    else:
                        ###多元组，将redis中多个集合合并
                        inter, s = get_pycombi(db, combi, class_no)
                    union = union | inter
                    #print "class = %d,py combi %s has %d"%(class_no, s, len(inter))
            else:    ###没有汉字没有英文没有数字
                continue
            compare_list = get_union_data(_pipe, class_no, union)
            for i in range(len(compare_list)):
                compare_unit = compare_list[i]
                his_name = compare_unit["name"].decode("utf-8")
                brand_no_his = compare_unit["no"]
                his_name_pinyin = compare_unit["py"]
                his_name_eng = compare_unit["eng"]
                his_name_pinyin = concate(his_name_pinyin, his_name_eng)
                his_name_china = compare_unit["ch"]
                his_name_bid = compare_unit["bid"]
                last_class[class_no] = compare_unit
                #start_time_s = datetime.datetime.now()
                py_judge = judge_pinyin(brand_name_pinyin, his_name_pinyin)
                #logger.info("====%s, %s, %s, %s, %s"%(brand_name, his_name, str(py_judge), str(brand_name_pinyin), his_name_pinyin) )
                if py_judge == False:
                    if len(brand_name_china) != len(his_name_china) or brand.glyphApproximation(brand_name_china, his_name_china) < 0.9:
                        continue
                #end_time_s = datetime.datetime.now()
                #cost_time_s = (end_time_s - start_time_s).total_seconds()
                #print "两商标计算拼音重合量的时间消耗为：", cost_time_s  #通常在1.5ms
                #start_time_c = datetime.datetime.now()
                similar, compare_Res = compute_similar(brand_name, his_name, gate)
                #if similar == True:
                #    logger.info(">>>>>%s,%s,%s,%s"%(brand_name, his_name, str(compare_Res), str(similar)))
                #else:
                #    logger.info("XXXXX%s,%s,%s,%s" % (brand_name, his_name, str(compare_Res), str(similar)))
                #end_time_c = datetime.datetime.now()
                #cost_time_c = (end_time_c - start_time_c).total_seconds()
                #print "两商标计算十种特征值的时间消耗为：", cost_time_c  ##通常在 100~ 150ms，取决于数据，也有2ms就算完的情况
                if similar == True:
                    similar_cnt[class_no] += 1 ###构造返回结果：近似商标名（及特征）
                    out_row = [brand_name, his_name, brand_no_his, class_no]
                    out_row.extend(compare_Res)
                    out_row.extend([compare_unit["sts"], '1', his_name_bid ])
                    return_list.append(out_row)
            del compare_list
    except:
        error_occur = True
        logger.error(u"检索数据时发生异常!!", exc_info=True)

    ###没有相似商标的情况
    try:
        for class_no in class_no_set:
            if similar_cnt[class_no] == 0:
                compare_unit = last_class[class_no]
                if compare_unit == None:
                    continue
                similar_cnt[class_no] += 1
                his_name = compare_unit["name"].decode("utf-8")
                brand_no_his = compare_unit["no"]
                his_name_bid = compare_unit["bid"]
                compare_Res = brand.getCharacteristics(brand_name, his_name)###构造返回结果：近似商标名（及特征）
                out_row = [brand_name, his_name, brand_no_his, class_no]
                out_row.extend(compare_Res)
                out_row.extend([compare_unit["sts"], '0', his_name_bid])
                return_list.append(out_row)
    except:
        error_occur = True
        logger.error(u"补充近似商标时发生异常!!", exc_info=True)

    try:  ###使用特征数据计算分类
        reload(trans_pre_data)
        itemList = getItemListOfBrand(return_list, item_dict, _pipe)  ###查同音商标名注册的商品项
        return_list = trans_pre_data.trans_pre_data_web(return_list, itemList, class_no_set, item_dict=item_dict)
    except:
        error_occur = True
        logger.error(u"特征值预测分类或构造返回结果时发生异常!!", exc_info=True)

    ###调用预测模块添东西
    end_time_c = datetime.datetime.now()
    cost_time_c = (end_time_c - start_time_c).total_seconds()
    logger.debug(u"进程%d 查询耗时为 :%s秒"%(os.getpid(), str(cost_time_c)) )
    return error_occur, return_list

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

##根据给定的商标id和大类。获取这个商标在这个大类下的小项id
def getItemListOfBrand(data_list , item_dict, _pipe):
    for brand in data_list:
        class_no = brand[3]
        bid = brand[-1]
        #brand[1] += ":bid=" + str(bid)
        item_key = item_key_prefix + str(class_no) + "::" + str(bid)
        _pipe.smembers(item_key)
    itemList = _pipe.execute()

    for i in range(len(data_list)):
        trans_itemList_i = []
        class_no = data_list[i][3]
        for item_no in itemList[i]:
            trans_itemList_i.append(item_dict[class_no][item_no])
        #print trans_itemList_i
        itemList[i] = trans_itemList_i
    return itemList

###判断两个商标中是否有同音字
def judge_pinyin(brand_name_pinyin, his_name_pinyin):
    b_list = brand_name_pinyin
    h_list = his_name_pinyin.split(",")
    b_len = len(b_list)
    h_len = len(h_list)

    cnt_comm = 0
    if b_len <= 3: ##商标长度小于等于3时，按乱序查找。即只要h串里有就行（可能重音，要标记）
        h_vis = [False] * (h_len)
        for i in range(b_len):
            for j in range(h_len):
                #print h_list[j], b_list[i], h_list[j] == b_list[i]
                if h_vis[j] == False and h_list[j] == b_list[i]:
                    cnt_comm += 1
                    h_vis[j] = True
                    break
    if b_len > 3:  ##商标长度大于等于3时，按正序查找（就是算最长匹配距离）
        cnt_comm = brand.maxMatchLen(b_list, h_list)

    #print "py check ===> ", b_list, h_list, cnt_comm
    if h_len > cnt_comm + 4:  ##字数比较，被比较商标与输入商标，在公有部分的基础上长4以上就pass
        return False

    if b_len < 3 and cnt_comm > 0 and h_len < cnt_comm + 2:
        # 输入商标的长度只有1或者2， 那么共有部分必须是1或者2
        return True
    elif b_len >= 3 and cnt_comm >= max(int(len(b_list) * 0.28), 2):  #
        #输入商标长度为3或者以上，那么部分重合就可以
        return True

    return False


###计算拼音共同下界
def compute_py_lowb(brand_name_pinyin):
    b_list = brand_name_pinyin

    if len(b_list) < 3:
        # print b_list, h_list, cnt_comm
        return max(len(b_list) - 1, 1)
    else:
        # print b_list,h_list
        return max(int(len(b_list) * 0.28), 2)

def compute_similar(brand_name, his_name, gate):
    compare_Res = brand.getCharacteristics(brand_name, his_name)
    similar = False
    for index in range(len(compare_Res)):
        if gate[index] == 'C':
            if len(brand_name) < 4 and compare_Res[index] >= 0.5:
                similar = True
            elif len(brand_name) >= 4 and compare_Res[index] >= 0.5:
                similar = True
        elif gate[index] == 'N':
            continue
        else:
            if compare_Res[index] >= gate[index]:
                similar = True
    return similar,  compare_Res

def concate(his_name_pinyin, his_name_eng):
    if len(his_name_pinyin) > 0:
        if len(his_name_eng) > 0:
            his_name_pinyin = his_name_pinyin + "," + his_name_eng
    elif len(his_name_eng) > 0:
        his_name_pinyin = his_name_eng
    return his_name_pinyin





