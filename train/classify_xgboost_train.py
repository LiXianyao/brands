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


"""
训练模型
1、读取目录下的训练集和验证集保存在的文件
2、使用参数进行训练
3、保存模型\参数配置
4、使用模型对验证集的数据进行预测
5、预测结果输出到csv文件
"""
def train_model( taskId, boost_parameters, train_parameters, train_statistics, model_id="N0001"):
    input_train_file_name = taskId + "_train"
    input_test_file_name = taskId + "_test"

    """读取训练集和验证集"""
    data_train = xgb.DMatrix('data/input/' + input_train_file_name)
    data_test = xgb.DMatrix('data/input/' + input_test_file_name)
    f_test = open('data/input/' + input_test_file_name + "_content")
    data_test_context = f_test.readlines()

    print("train file size is: ",data_train.num_col(), data_train.num_row())
    print("test file size is: ", data_test.num_col(), data_test.num_row())
    # print( type(data_train))
    taskId += "_" + model_id
    """............................................."""
    # 设置默认模型参数列表
    # eta是用来防止过拟合的因子（公式中的v）,为了防止过拟合，更新过程中用到的收缩步长。在每次提升计算之后，算法
    # 会直接获得新特征的权重。 eta通过缩减特征的权重使提升计算过程更加保守。缺省值为0.3
    # ，silent表示是否需要输出无关中间信息，objective是使用这个模型的目标
    deafault_boost_parameters = {'max_depth': 5, 'eta': 0.25, 'silent': 1, 'objective': 'multi:softprob',
                                 'num_class': 2, 'subsample': 0.8}

    # 使用传参文件里定义的配置来更新这个默认配置表
    deafault_boost_parameters.update(boost_parameters)

    """............................................."""
    # 设置默认任务参数列表
    #
    deafault_train_parameters = {'num_boost_round': 450, 'early_stopping_rounds': 30}

    # 使用传参文件里定义的配置来更新这个默认配置表
    deafault_train_parameters.update(train_parameters)

    watchlist = [(data_test, 'eval'), (data_train, 'train')]  # 使用训练集和测试集一起检测模型当前的错误率
    print "boost_parameters ars :%s" % deafault_boost_parameters
    print "train_parameters ars :%s" % deafault_train_parameters
    """训练"""
    bst = xgb.train(deafault_boost_parameters, data_train, num_boost_round=deafault_train_parameters['num_boost_round']
                    , evals=watchlist, early_stopping_rounds=deafault_train_parameters['early_stopping_rounds'])

    # 保存模型
    model_name = taskId + '.model'
    """保存模型"""
    bst.save_model('models/' + model_name)
    #保存参数
    f_param = open('models/' + taskId + '_parameters.txt',"w")
    f_param.write(str(deafault_boost_parameters) + '\n')
    f_param.write(str(deafault_train_parameters) + '\n')
    f_param.close()

    """用验证集进行预测
    y_hat是预测结果矩阵，每行有标签数个小数，和为1.0
    y是训练数据里写的，原标签的号数（0开始）
    """
    nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
    start_time_s = datetime.datetime.now()
    y_hat = bst.predict(data_test)
    endtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
    end_time_s = datetime.datetime.now()
    cost_time_s = (end_time_s - start_time_s).microseconds
    y = data_test.get_label()

    csv_name = taskId + "_test_result.csv"
    f_out = open('testRes/' + csv_name,"w")
    writer = csv.writer(f_out)
    title = [u"预测分类",u"预测概率",u"原标签",u"分类与原标签是否匹配",u"输入商标名称", u"历史商标名称", u"所属小项"]
    for i in range(len(title)):
        title[i] = title[i].encode("gbk")
    writer.writerow(title)

    test_size = data_test.num_row()
    error = 0
    pro_list = [90.0, 80.0, 70.0, 60.0, 50.0]
    cnt_list = [0, 0, 0, 0, 0]
    cnt_false_list = [0, 0, 0, 0, 0]
    TP, TN, FP =  [0,0], [0,0], [0,0]
    for i in range(test_size):
        temp = dict(zip(range(2), y_hat[i]))
        temp = sorted(temp.iteritems(), key=lambda d: d[1], reverse=True)
        #if i < 20:
        #   print "y[i]=",int(y[i]),temp
        res = '1'
        out_row = []
        out_row.extend([temp[0][0], round(temp[0][1] * 100.0, 2)])  ##u"预测分类",u"预测概率"
        if i < 0:
            print data_test_context[i]
            print "i", i, "predict = ", temp[0][0], round(temp[0][1] * 100.0, 2)
        addcnt(round(temp[0][1] * 100.0, 2), pro_list, cnt_list) ##总体概率分布计数
        if int(y[i]) != temp[0][0]:
            addcnt(round(temp[0][1] * 100.0, 2), pro_list, cnt_false_list) ###错误概率分布计数
            error += 1
            res = '0'
            TN[temp[0][0]] += 1 ##被分类为y但是其实不是y的数量
            FP[int(y[i])] += 1 ##实际是y但是没有被分为y的数量
        else: ##实际分类与预测一致
            TP[int(y[i])] += 1
        out_row.extend([int(y[i]), res])
        this_name = data_test_context[i].split("&*(")[0]
        his_name = data_test_context[i].split("&*(")[1]
        product_no = data_test_context[i].split("&*(")[-1]
        out_row.extend([this_name, his_name, product_no])
        writer.writerow(out_row)

    f_out.close()
    f_test.close()
    # error = sum( int(y != (y_hat > 0.5)))
    error_rate = float(error) / len(y_hat)

    f_record = open("testRes/Model_" + model_name + ".record","w")
    f_record.write("模型名：%s"%model_name)
    f_record.write("测试文件名: %s"%input_test_file_name)
    print '验证样本总数：\t', len(y_hat)
    f_record.write('验证样本总数：\t' + str(len(y_hat)) + "\n")
    print '错误条数：\t%4d' % error
    f_record.write('错误条数：\t%4d' % error + "\n")
    print '错误率：\t%.5f%%' % (100 * error_rate)
    f_record.write('错误率：\t%.5f%%' % (100 * error_rate)+ "\n")
    print "start at %s, end at %s" % (nowtime, endtime)
    f_record.write("start at %s, end at %s" % (nowtime, endtime))
    print "类别1的 TP=%d, TN=%d, FP=%d, 准确率=%.2f, 召回率=%.2f" % \
          (TP[1], TN[1], FP[1], float(TP[1])/(TP[1] + TN[1]), float(TP[1])/(TP[1] + FP[1]))
    f_record.write("类别1的 TP=%d, TN=%d, FP=%d, 准确率=%.2f, 召回率=%.2f" % \
          (TP[1], TN[1], FP[1], float(TP[1])/(TP[1] + TN[1]), float(TP[1])/(TP[1] + FP[1])))
    print "类别0的 TP=%d, TN=%d, FP=%d, 准确率=%.2f, 召回率=%.2f" % \
          (TP[0], TN[0], FP[0], float(TP[0]) / (TP[0] + TN[0]), float(TP[0]) / (TP[0] + FP[0]))
    f_record.write("类别0的 TP=%d, TN=%d, FP=%d, 准确率=%.2f, 召回率=%.2f" % \
                   (TP[0], TN[0], FP[0], float(TP[0]) / (TP[0] + TN[0]), float(TP[0]) / (TP[0] + FP[0])))

    #####输出各种指标到csv文件
    csv_name = train_statistics
    title = []
    if os.path.exists(csv_name) == False:
        ###文件不存在，则把表头写一下
        title = [u"taskId", u"num_round", u"max_depth", u"subsample", u"colsample_bytree", u"colsample_bylevel",
                 u"total_example", u"error_cnt", u"error_rate", u"start_time", u"end_time", u"cost_time_s",
                 u"pro>=90_cnt", u"pro>=90_cnt_rate", u"pro>=90_error", u"pro>=90_error_rate",
                 u"pro>=80_cnt", u"pro>=80_cnt_rate", u"pro>=80_error", u"pro>=80_error_rate",
                 u"pro>=70_cnt", u"pro>=70_cnt_rate", u"pro>=70_error", u"pro>=70_error_rate",
                 u"pro>=60_cnt", u"pro>=60_cnt_rate", u"pro>=60_error", u"pro>=60_error_rate",
                 u"pro>=50_cnt", u"pro>=50_cnt_rate", u"pro>=50_error", u"pro>=50_error_rate"]
        for i in range(len(title)):
            title[i] = title[i].encode("gbk")
    f_out = open(csv_name, "a")
    writer = csv.writer(f_out)
    if len(title) != 0:
        writer.writerow(title)
    statistics_row = [str(taskId), deafault_train_parameters["num_boost_round"], deafault_boost_parameters["max_depth"],
                      deafault_boost_parameters["subsample"], deafault_boost_parameters["colsample_bytree"], deafault_boost_parameters["colsample_bylevel"],
                      len(y_hat), error, error_rate, start_time_s, end_time_s, cost_time_s]

    for i in range(len(pro_list)):
        if i>0:
            cnt_list[i] += cnt_list[i-1]
            cnt_false_list[i] += cnt_false_list[i-1]
        cnt_rate = float(cnt_list[i])/len(y_hat)
        if cnt_list[i] > 0:
            error_false = float(cnt_false_list[i])/cnt_list[i]
        else:
            error_false = 0.0
        print "预测可信度大于%.1f的商标条数共有%d条，其中%d条是错误的，错误率%.2f\n" % (pro_list[i],cnt_list[i],cnt_false_list[i],error_false)
        f_record.write("预测可信度大于%f的商标条数共有%d条，其中%d条是错误的，错误率%f\n" % (pro_list[i],cnt_list[i],cnt_false_list[i],error_false))
        statistics_row.extend([cnt_list[i], cnt_rate, cnt_false_list[i], error_false])
    writer.writerow(statistics_row)
    f_out.close()

    return model_name, csv_name


"""
训练模型
1、读取目录下的训练集和验证集保存在的文件
2、使用参数进行训练
3、保存模型\参数配置
4、使用模型对验证集的数据进行预测
5、预测结果输出到csv文件
"""
def test_model( taskId, model_name):
    input_test_file_name = taskId + "_test"

    """验证集"""
    data_test = xgb.DMatrix('data/input/' + input_test_file_name)
    f_test = open('data/input/' + input_test_file_name + "_content")
    data_test_context = f_test.readlines()

    print("test file size is: ", data_test.num_col(), data_test.num_row())

    """保存模型"""
    bst = xgb.Booster()
    bst.load_model(model_name)


    """用验证集进行预测
    y_hat是预测结果矩阵，每行有标签数个小数，和为1.0
    y是训练数据里写的，原标签的号数（0开始）
    """
    nowtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
    start_time_s = datetime.datetime.now()
    y_hat = bst.predict(data_test)
    endtime = time.strftime("%Y%m%d%H%M%S", time.localtime())
    end_time_s = datetime.datetime.now()
    cost_time_s = (end_time_s - start_time_s).microseconds
    y = data_test.get_label()

    csv_name = taskId + "_test_result.csv"
    f_out = open('testRes/' + csv_name,"w")
    writer = csv.writer(f_out)
    title = [u"预测分类",u"预测概率",u"原标签",u"分类与原标签是否匹配",u"输入商标名称", u"历史商标名称", u"所属小项"]
    for i in range(len(title)):
        title[i] = title[i].encode("gbk")
    writer.writerow(title)

    test_size = data_test.num_row()
    error = 0
    pro_list = [90.0, 80.0, 70.0, 60.0, 50.0]
    cnt_list = [0, 0, 0, 0, 0]
    cnt_false_list = [0, 0, 0, 0, 0]
    TP, TN, FP =  [0,0], [0,0], [0,0]
    for i in range(test_size):
        temp = dict(zip(range(2), y_hat[i]))
        temp = sorted(temp.iteritems(), key=lambda d: d[1], reverse=True)
        #if i < 20:
        #   print "y[i]=",int(y[i]),temp
        res = '1'
        out_row = []
        out_row.extend([temp[0][0], round(temp[0][1] * 100.0, 2)])  ##u"预测分类",u"预测概率"
        if i < 0:
            print data_test_context[i]
            print "i", i, "predict = ", temp[0][0], round(temp[0][1] * 100.0, 2)
        addcnt(round(temp[0][1] * 100.0, 2), pro_list, cnt_list) ##总体概率分布计数
        if int(y[i]) != temp[0][0]:
            addcnt(round(temp[0][1] * 100.0, 2), pro_list, cnt_false_list) ###错误概率分布计数
            error += 1
            res = '0'
            TN[temp[0][0]] += 1 ##被分类为y但是其实不是y的数量
            FP[int(y[i])] += 1 ##实际是y但是没有被分为y的数量
        else: ##实际分类与预测一致
            TP[int(y[i])] += 1
        out_row.extend([int(y[i]), res])
        this_name = data_test_context[i].split("&*(")[0]
        his_name = data_test_context[i].split("&*(")[1]
        product_no = data_test_context[i].split("&*(")[-1]
        out_row.extend([this_name, his_name, product_no])
        writer.writerow(out_row)

    f_out.close()
    f_test.close()
    # error = sum( int(y != (y_hat > 0.5)))
    error_rate = float(error) / len(y_hat)

    model_name = model_name.split("/")[-1].split(".")[0]
    f_record = open("testRes/test_" + model_name + "_with_data_" + taskId +  ".record","w")
    f_record.write("模型名：%s"%model_name)
    f_record.write("测试文件名: %s"%input_test_file_name)
    print '验证样本总数：\t', len(y_hat)
    f_record.write('验证样本总数：\t' + str(len(y_hat)) + "\n")
    print '错误条数：\t%4d' % error
    f_record.write('错误条数：\t%4d' % error + "\n")
    print '错误率：\t%.5f%%' % (100 * error_rate)
    f_record.write('错误率：\t%.5f%%' % (100 * error_rate)+ "\n")
    print "start at %s, end at %s" % (nowtime, endtime)
    f_record.write("start at %s, end at %s" % (nowtime, endtime))
    print "类别1的 TP=%d, TN=%d, FP=%d, 准确率=%.2f, 召回率=%.2f" % \
          (TP[1], TN[1], FP[1], float(TP[1])/(TP[1] + TN[1]), float(TP[1])/(TP[1] + FP[1]))
    f_record.write("类别1的 TP=%d, TN=%d, FP=%d, 准确率=%.2f, 召回率=%.2f" % \
          (TP[1], TN[1], FP[1], float(TP[1])/(TP[1] + TN[1]), float(TP[1])/(TP[1] + FP[1])))
    print "类别0的 TP=%d, TN=%d, FP=%d, 准确率=%.2f, 召回率=%.2f" % \
          (TP[0], TN[0], FP[0], float(TP[0]) / (TP[0] + TN[0]), float(TP[0]) / (TP[0] + FP[0]))
    f_record.write("类别0的 TP=%d, TN=%d, FP=%d, 准确率=%.2f, 召回率=%.2f" % \
                   (TP[0], TN[0], FP[0], float(TP[0]) / (TP[0] + TN[0]), float(TP[0]) / (TP[0] + FP[0])))

    for i in range(len(pro_list)):
        if i>0:
            cnt_list[i] += cnt_list[i-1]
            cnt_false_list[i] += cnt_false_list[i-1]
        cnt_rate = float(cnt_list[i])/len(y_hat)
        if cnt_list[i] > 0:
            error_false = float(cnt_false_list[i])/cnt_list[i]
        else:
            error_false = 0.0
        print "预测可信度大于%.1f的商标条数共有%d条，其中%d条是错误的，错误率%.2f\n" % (pro_list[i],cnt_list[i],cnt_false_list[i],error_false)
        f_record.write("预测可信度大于%f的商标条数共有%d条，其中%d条是错误的，错误率%f\n" % (pro_list[i],cnt_list[i],cnt_false_list[i],error_false))

    return model_name, csv_name

def addcnt(prob, prob_list, cnt_list):
    for i in range(len(prob_list)):
        prob_g = prob_list[i]
        if prob >= prob_g:
            cnt_list[i] += 1
            return

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
    boost_parameters_sec = cf.options("boost_parameter")
    train_parameters_sec = cf.options("train_parameter")
    action_parameters_sec = cf.options("action_parameter")
    statistics_file_sec = cf.options("statistics_file")

    boost_parameters_dict = config_from_sec(boost_parameters_sec, cf, "boost_parameter")
    train_parameters_dict = config_from_sec(train_parameters_sec, cf, "train_parameter")
    action_parameters_dict = config_from_sec(action_parameters_sec, cf, "action_parameter")
    statistics_files_dict = config_from_sec(statistics_file_sec, cf, "statistics_file")
    print statistics_files_dict
    return boost_parameters_dict, train_parameters_dict, action_parameters_dict, statistics_files_dict

"""脚本的命令行输入提示"""
def printUsage():
    print "usage: classify_xgboost_offline_train.py -f <configFileName> -a [test|train]"

if __name__ == "__main__":
    """这个主调用
    用途是读 构造好了的输入文件 ，然后训练模型/预测输出csv
    所谓 构造好了的输入文件，是经由csvHandling.py运行后产生的输入文件，记下他的时间前缀
    它存在的意义是为了不用反复跑csvHandling.py
    因为本身可以用同一组输入文件跑出很多个不同参数的模型，每次测的时候都从csvHandlling.py跑太慢了，输入文件是一样的（模型词典不变的情况下）
    直接在这里改参数，就可以跑一系列输入文件相同但参数不同的模型
    """

    try:
        opts, args = getopt.getopt(sys.argv[1:],"a:f:",["action=","file="])
    except getopt.GetoptError:
        #参数错误
        printUsage()
        sys.exit(-1)

    action = "train"
    configFile = "train_models.config"
    for opt,arg in opts:
        if opt in ("-a","--action"):
            action = arg
        elif opt in ("-f","--file"):
            configFile = arg
    print action
    boost_parameters_dict, train_parameters_dict, action_parameters_dict, statistics_files_dict = load_config_file(configFile)  # 读取训练配置文件
    if action == 'train':
        print "now train:"
        train_model(str(action_parameters_dict['time_stamp']), boost_parameters_dict, train_parameters_dict, statistics_files_dict['train_statistics'],model_id=str(action_parameters_dict['model_id']))
    elif action == 'test':
        print "now test:"
        test_model(str(action_parameters_dict['test_data_id']), str(action_parameters_dict['test_model']))