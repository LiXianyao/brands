#-*-coding:utf8-*-#
import sys
import  ConfigParser
import os,csv
import getopt
import time
import numpy as np
import datetime
import classify_xgboost_prediction
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )

attribute_title = [u"汉字编辑距离相似度", u"拼音相似度", u"汉字包含被包含",
         u"汉字排列组合", u"汉字含义相近", u"汉字字形相似度", u"英文编辑距离相似度", u"英文包含被包含",
         u"英文排列组合", u"数字完全匹配"]

def trans_Data(taskId, data_list, item_list, class_no_set):
    input_file_name = taskId + "_input"
    input_lines = []
    for line in data_list:
        ##单条的特征值
        attribute = line[4: 14]
        ##历史商标状态
        his_status = line[14]

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
    ###调用模型计算得到每个商标的通过率
    reload(classify_xgboost_prediction)
    predict_res = classify_xgboost_prediction.request_from_web("data/" + input_file_name)
    #print data_list, predict_res
    os.system("rm " + "data/" + input_file_name)
    ###统计计算每个大类的通过率
    return compute_class_through_rate(data_list, item_list, predict_res, class_no_set)

###统计计算每个大类的通过率
def  compute_class_through_rate(data_list, item_list, predict_res, class_no_set):
    res_dict = {}
    for class_no in class_no_set:
        res_dict[class_no] = {u"商标名字":"", u"所属类别": class_no, u"近似名字组": [], u"名字近似度列表":[], u"商品项及注册成功率列表":{}}
    for i in range(len(data_list)):
        this_name = data_list[i][0]
        his_name = data_list[i][1]
        class_no = data_list[i][3]
        ##单条的特征值
        attribute = data_list[i][4: 14]
        res_dict[class_no][u"商标名字"] = this_name
        res_dict[class_no][u"近似名字组"].append(his_name)
        max_attri = max(attribute)
        max_attri_index = attribute.index(max_attri)
        res_dict[class_no][u"名字近似度列表"].append(his_name + " " + attribute_title[max_attri_index] + " " + str(max_attri * 100.0) + "%")
        for item in item_list[i]:
            if res_dict[class_no][u"商品项及注册成功率列表"].has_key(item) == False:
                res_dict[class_no][u"商品项及注册成功率列表"][item] = predict_res[i][1]
            else:
                res_dict[class_no][u"商品项及注册成功率列表"][item] = min(predict_res[i][1], res_dict[class_no][u"商品项及注册成功率列表"][item])

    for class_no in class_no_set:
        for item in res_dict[class_no][u"商品项及注册成功率列表"].keys():
            res_dict[class_no][u"商品项及注册成功率列表"][item] = str(res_dict[class_no][u"商品项及注册成功率列表"][item]) + "%"
    return res_dict

def save_input_file(input_lines, input_file_name):
    with open(input_file_name, "w") as input_file:
        for input_line in input_lines:
            line = input_line
            input_file.write(line)

###将web请求筛选计算后的特征转成输入数据
def trans_pre_data_web(data_list, item_list, class_no_set):
    taskId = datetime.datetime.now().strftime('%H%M%S%f')
    return trans_Data(taskId, data_list, item_list, class_no_set)