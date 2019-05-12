#-*-coding:utf8-*-#
import sys
import os
from pypinyin import lazy_pinyin, Style
from itertools import combinations
import datetime
import trans_pre_data
from similarity import brand, strFunction, compute
sys.path.append("..")
reload(sys)
sys.setdefaultencoding("utf-8")

###redis数据库 的前缀
data_key_prefix = "bData::"
pyset_key_prefix = "bPySet::"  # set
item_key_prefix = "bItem::"


###获取商标的群组和小项名字、编号的映射关系
def load_brand_item():
    from dataStorage import brand_item
    item_list = brand_item.BrandItem.query.all()
    #brand_item.db_session.rollback()
    item_dict = {}
    for item in item_list:
        item_name = item.item_name
        item_no = item.item_no
        class_no = int(item.class_no)
        if not item_dict.has_key(class_no):
            item_dict[class_no] = {}
        item_dict[class_no][item_no] = (item_no, item_name)
    del brand_item
    return item_dict


###redis数据库
def form_pre_data_flask(input_json, item_dict, db, _pipe, logger):
    brand_name = input_json["name"]
    brand_name_china = strFunction.get_china_str(brand_name)
    brand_name_pinyin = lazy_pinyin(brand_name_china, style=Style.TONE3)
    brand_name_num , brand_name_eng, character_set = strFunction.get_not_china_list(brand_name)
    brand_name_pinyin.extend(brand_name_eng)
    class_no_set = input_json["categories"]
    logger.debug("brand name is %s, with searching class: %s" % (brand_name, str(class_no_set)))
    error_occur = False ###标记运行期间是否发生错误

    similar_cnt = {k: v for k, v in zip(class_no_set, [0]*len(class_no_set))}  ##累计每个类别找到的近似商标数
    last_class = {k: v for k, v in zip(class_no_set, [None]*len(class_no_set))}  ##保存每个类别的近似商标
    start_time_c = datetime.datetime.now()

    return_list = []
    py_low = compute.compute_py_lowb(brand_name_pinyin + character_set)##根据长度确定确定排列组合的下界
    py_combi = combinations(brand_name_pinyin, py_low)
    combi_store = set()
    try:
        for class_no in class_no_set:
            if py_low > 0:
                #共有拼音排列组合
                union = set()
                for combi in py_combi:
                    combi_store.add(combi)
                    if len(combi) == 1:
                        inter = db.smembers(pyset_key_prefix + str(class_no) + "::" + combi[0])
                        #s = combi[0]
                    else:
                        ###多元组，将redis中多个集合合并
                        inter, s = get_pycombi(db, combi, class_no)
                    union = union | inter
                    #print "class = %d,py combi %s has %d"%(class_no, s, len(inter))
            else:    ###没有汉字没有英文没有数字
                continue
            py_combi = combi_store
            compare_list = get_union_data(_pipe, class_no, union)
            for i in range(len(compare_list)):
                compare_unit = compare_list[i]
                his_name = compare_unit["name"].decode("utf-8")
                brand_no_his = compare_unit["no"]
                his_name_pinyin = compare_unit["py"]
                his_name_eng = compare_unit["eng"]
                his_name_pinyin = strFunction.concate(his_name_pinyin, his_name_eng)
                his_name_china = compare_unit["ch"]
                his_name_bid = compare_unit["bid"]
                last_class[class_no] = compare_unit
                start_time_s = datetime.datetime.now()
                py_judge = compute.judge_pinyin(brand_name_pinyin, his_name_pinyin)
                #logger.info("====%s, %s, %s, %s, %s"%(brand_name, his_name, str(py_judge), str(brand_name_pinyin), his_name_pinyin) )
                if py_judge == False:
                    if len(brand_name_china) != len(his_name_china) or brand.glyphApproximation(brand_name_china, his_name_china) < 0.9:
                        continue
                #end_time_c = datetime.datetime.now()
                #cost_time_c = (end_time_c - start_time_c).total_seconds()
                #print u"两商标计算拼音近似过滤的时间消耗为：", cost_time_c  ##通常在 100~ 150ms，取决于数据，也有2ms就算完的情况
                similar, compare_Res = compute.compute_similar(brand_name, his_name)
                #if similar == True:
                #    logger.info(">>>>>%s,%s,%s,%s"%(brand_name, his_name, str(compare_Res), str(similar)))
                #else:
                #    logger.info("XXXXX%s,%s,%s,%s" % (brand_name, his_name, str(compare_Res), str(similar)))
                #print u"两商标计算十种特征值的时间消耗为：", cost_time_c  ##通常在 100~ 150ms，取决于数据，也有2ms就算完的情况
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
        itemList = getItemListOfBrand(return_list, item_dict, _pipe, logger)  ###查同音商标名注册的商品项
        return_list = trans_pre_data.trans_pre_data_web(return_list, itemList, class_no_set, item_dict=item_dict)
        for categoryResult in return_list.values():
            logger.debug(u"类别 %d 有 %d 条近似商标名" % (categoryResult.getInfo()))
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
    first_key = pyset_key_prefix + str(class_no) + "::" + combi[0]
    combi_str = combi[0]

    for i in range(1, combi_len):
        set_key = pyset_key_prefix + str(class_no) + "::" + combi[i]
        combi_str += "," + combi[i]
        inter_args.append(set_key)

    inter = db.sinter(first_key, *tuple(inter_args))
    return inter, combi_str

##根据给定的商标id和大类。获取这个商标在这个大类下的小项id
def getItemListOfBrand(data_list , item_dict, _pipe, logger):
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
            try:
                trans_itemList_i.append(item_dict[class_no][item_no])
            except:
                logger.debug(u"执行异常，未命中商品项数据： 国际分类%s, 商品项编号%s" % (str(class_no), str(item_no)))
        itemList[i] = trans_itemList_i
    return itemList





