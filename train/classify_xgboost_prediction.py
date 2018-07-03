# -*- coding:utf-8 -*-#
import json
import re
import csv
import xgboost as xgb
import getopt
import sys
import ConfigParser
import time
import datetime
import os

reload(sys)
sys.setdefaultencoding('utf-8')
label_num = 2

"""
用途是读 构造好了的输入文件 ，然后预测输出csv
重点是上面这个，构造好了的输入文件，是经由csvHandling.py运行后产生的，并且和调用这个函数时用的模型的 模型词典 相同，才能使用
它存在的意义是为了不用反复跑csvHandling.py
因为本身可以用同一组输入文件跑出很多个不同参数的模型，每次测的时候都从csvHandlling.py跑太慢了，输入文件是一样的（模型词典不变的情况下）
"""
def test_model(action_parameters_dict, input_file_name):
    # 加载模型
    model_name = action_parameters_dict["model_name"]
    print model_name

    bst = xgb.Booster()
    bst.load_model("../train/models/" + model_name)

    data_test = xgb.DMatrix(input_file_name)
    print("test file size is: ", data_test.num_col(), data_test.num_row())
    # print( type(data_train))

    """预测
    y_hat是预测结果矩阵，每行有标签数个小数，和为1.0
    y是训练数据里写的，原标签的号数（0开始）
    """
    nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
    start_time_s = datetime.datetime.now()
    y_hat = bst.predict(data_test)
    endtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
    end_time_s = datetime.datetime.now()
    cost_time_s = (end_time_s - start_time_s).microseconds
    test_size = data_test.num_row()

    predict_res = []
    for i in range(test_size):
        temp = dict(zip(range(label_num), y_hat[i]))
        #print temp
        through_rate = round(temp[1] * 100.0, 2)
        if through_rate < 50.0:
            predict_res.append([False, through_rate])
        else:
            predict_res.append([True, through_rate])

    return predict_res

"""读取配置文件-反复用的一部分代码段"""
def config_from_sec(sec_list, cf, section_name):
    config_dict ={}
    for opt_name in sec_list:
        opt_value = cf.get(section_name,opt_name)
        #这一步的配置值都是字符串，要转类型
        try:
            config_dict[opt_name] = int(opt_value)
        except ValueError,e: #不是整数，试下小数
            try:
                config_dict[opt_name] = float(opt_value)
            except ValueError, e:  # 不是小数，那就还是字符串
                config_dict[opt_name] = opt_value
    return config_dict

"""读取训练配置文件"""
def load_config_file(configFile):
    cf = ConfigParser.ConfigParser()
    cf.read(configFile)
    action_parameters_sec = cf.options("action_parameter")
    action_parameters_dict = config_from_sec(action_parameters_sec, cf, "action_parameter")
    return action_parameters_dict

def request_from_web(input_file_name):
    configFile = "predict_models.config"
    action_parameters_dict= load_config_file(configFile)  # 读取训练配置文件
    return test_model(action_parameters_dict, input_file_name)