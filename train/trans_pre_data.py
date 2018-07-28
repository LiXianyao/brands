#-*-coding:utf8-*-#
import sys
import os
import datetime
import classify_xgboost_prediction
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )

def trans_Data(taskId, data_list, item_list, class_no_set):
    input_file_name = taskId + "_input"
    input_lines = []
    for line in data_list:
        ##单条的特征值
        attribute = line[4: 14]

        input_line = "0"
        for i in range(len(attribute)):
            attri = attribute[i]
            if attri == "nan":
                print "catch None"
                attri = str(0.0)
            input_line += " " + str(i) + ":" + str(attri)
        input_line += "\n"
        input_lines.append(input_line)

    save_input_file(input_lines,"data/" + input_file_name)
    if len(input_lines) > 0:
        ###调用模型计算得到每个商标的通过率
        reload(classify_xgboost_prediction)
        predict_res = classify_xgboost_prediction.request_from_web("data/" + input_file_name)
        #print data_list, predict_res
        os.system("rm " + "data/" + input_file_name)
    else:
        predict_res = []
    ###统计计算每个大类的通过率
    return compute_class_through_rate_resEntity(data_list, item_list, predict_res, class_no_set)

###统计计算每个大类的通过率
def  compute_class_through_rate(data_list, item_list, predict_res, class_no_set):
    res_dict = {}
    similar_name_list = {}
    import similarName
    reload(similarName)
    from similarName import similarName
    for class_no in class_no_set:
        res_dict[class_no] = {u"商标名字":"", u"所属类别": class_no, u"近似名字组": [], u"名字近似度列表":[], u"商品项及注册成功率列表":{}, u"近似名字标签":[]}
        similar_name_list[class_no] = []

    for i in range(len(data_list)):
        this_name = data_list[i][0]
        his_name = data_list[i][1]
        his_no = data_list[i][2]
        class_no = data_list[i][3]
        res_dict[class_no][u"商标名字"] = this_name
        ##单条的特征值
        attribute = data_list[i][4: 14]
        similar_name_list[class_no].append( similarName(compareName=this_name, name=his_name, register_no=his_no, attriList=attribute))

        for (item_no, item_name) in item_list[i]:
            item = str(item_no) + " " + str(item_name)
            if res_dict[class_no][u"商品项及注册成功率列表"].has_key(item) == False:
                res_dict[class_no][u"商品项及注册成功率列表"][item] = [predict_res[i][1], his_name]
            elif res_dict[class_no][u"商品项及注册成功率列表"][item][0] > predict_res[i][1]:
                res_dict[class_no][u"商品项及注册成功率列表"][item] = [predict_res[i][1], his_name]

    for class_no in class_no_set:
        ##近似列表排序
        similar_name_list[class_no].sort(key = lambda similarName:(similarName.tag, similarName.rate))
        for similarNameUnit in  similar_name_list[class_no]:
            res_dict[class_no][u"近似名字组"].append(similarNameUnit.name)
            res_dict[class_no][u"名字近似度列表"].append(similarNameUnit.name + " " + str(similarNameUnit.rate) + "%")
            res_dict[class_no][u"近似名字标签"].append(similarNameUnit.name + " " + similarNameUnit.tagName)

        ##调整成功率列表的结果
        for item in res_dict[class_no][u"商品项及注册成功率列表"].keys():
            res_dict[class_no][u"商品项及注册成功率列表"][item] = str(res_dict[class_no][u"商品项及注册成功率列表"][item][0]) + "%   " \
                                                                            "" + res_dict[class_no][u"商品项及注册成功率列表"][item][1]
    return res_dict


###统计计算每个大类的通过率
def compute_class_through_rate_resEntity(data_list, item_list, predict_res, class_no_set):
    goodsRate_dict = {}
    similar_name_list = {}
    import similarName, CategoryRetrievalResult, goodsRegisterRate
    reload(similarName)
    reload(CategoryRetrievalResult)
    reload(goodsRegisterRate)
    from similarName import similarName
    from CategoryRetrievalResult import CategoryRetrievalResult
    from goodsRegisterRate import goodsRegisterRate
    for class_no in class_no_set:
        goodsRate_dict[class_no] = {}
        similar_name_list[class_no] = []

    for i in range(len(data_list)):
        this_name = data_list[i][0]
        his_name = data_list[i][1]
        his_no = data_list[i][2]
        class_no = data_list[i][3]
        ##单条的特征值
        attribute = data_list[i][4: 14]
        similar_name_list[class_no].append( similarName(compareName=this_name, name=his_name, register_no=his_no, attriList=attribute))

        for (item_no, item_name) in item_list[i]:
            if goodsRate_dict[class_no].has_key(item_no) == False:
                goodsRate_dict[class_no][item_no] = goodsRegisterRate(item_no, item_name, predict_res[i][1], his_name)
            else:
                goodsRate_dict[class_no][item_no].updateRate(predict_res[i][1], his_name)

    res_dict = {}
    for class_no in class_no_set:
        ##近似列表排序
        similar_name_list[class_no].sort(key = lambda similarName:(similarName.tag, similarName.rate))
        categoryResult = CategoryRetrievalResult(category= class_no,
                                                 similarNameList= similar_name_list[class_no],
                                                 goodsRegisterRateList= goodsRate_dict[class_no].values())
        res_dict[class_no] = categoryResult
    #print "res_dict =", res_dict
    return res_dict

def save_input_file(input_lines, input_file_name):
    with open(input_file_name, "w") as input_file:
        for input_line in input_lines:
            line = input_line
            input_file.write(line)

###将web请求筛选计算后的特征转成输入数据
def trans_pre_data_web(data_list, item_list, class_no_set):
    taskId = str(os.getpid())
    return trans_Data(taskId, data_list, item_list, class_no_set)