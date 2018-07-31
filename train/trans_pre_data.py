#-*-coding:utf8-*-#
import sys
import os
import datetime
import classify_xgboost_prediction
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )

def trans_Data(taskId, data_list, item_list, class_no_set, item_dict={}):
    input_file_name = taskId + "_input"
    input_lines = []
    for line in data_list:
        ##单条的特征值
        attribute = line[4: 14]

        input_line = "0"
        for i in range(len(attribute)):
            attri = attribute[i]
            if attri == "nan":
                attri = str(0.0)
            input_line += " " + str(i) + ":" + str(attri)
        input_line += "\n"
        input_lines.append(input_line)

    save_input_file(input_lines,"data/" + input_file_name)
    if len(input_lines) > 0:###调用模型计算得到每个商标的通过率
        #reload(classify_xgboost_prediction)
        predict_res = classify_xgboost_prediction.request_from_web("data/" + input_file_name)
    else:  ##没有特征数据
        predict_res = []
    ###统计计算每个大类的通过率
    return compute_class_through_rate_resEntity(data_list, item_list, predict_res, class_no_set, item_dict)

###统计计算每个大类的通过率
def compute_class_through_rate_resEntity(data_list, item_list, predict_res, class_no_set, item_dict):
    goodsRate_dict = {}
    similar_name_list = {}
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
                goodsRate_dict[class_no][item_no] = goodsRegisterRate(item_no, item_name, predict_res[i][1])#, his_name)
            else:
                goodsRate_dict[class_no][item_no].updateRate(predict_res[i][1])#, his_name)

    res_dict = {}
    for class_no in class_no_set:
        ##近似列表排序
        similar_name_list[class_no].sort(key = lambda similarName:(similarName.tag, similarName.rate))
        ##近似名字没有包含的商品项添加到结果里
        for item_no in item_dict[class_no].keys():
            item_name = item_dict[class_no][item_no][1]
            if goodsRate_dict[class_no].has_key(item_no) == False:
                goodsRate_dict[class_no][item_no] = goodsRegisterRate(item_no, item_name)
        categoryResult = CategoryRetrievalResult(category= class_no,
                                                 similarNameList= similar_name_list[class_no],
                                                 goodsRegisterRateList= goodsRate_dict[class_no].values())
        res_dict[class_no] = categoryResult
    return res_dict


###特征数据保存为文件
def save_input_file(input_lines, input_file_name):
    with open(input_file_name, "w") as input_file:
        for input_line in input_lines:
            line = input_line
            input_file.write(line)

###将web请求筛选计算后的特征转成输入数据
def trans_pre_data_web(data_list, item_list, class_no_set, item_dict={}):
    taskId = str(os.getpid())
    return trans_Data(taskId, data_list, item_list, class_no_set, item_dict=item_dict)