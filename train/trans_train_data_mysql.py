#-*-coding:utf8-*-#
import sys
import  ConfigParser
import json
import getopt
import time
import numpy as np
import math
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )

u"""时间戳！！用于区分同一套模型/训练数据/测试数据"""
nowtime = time.strftime("%Y%m%d%H%M", time.localtime())
configFile = "train.config"
taskId = "default_train_" + nowtime

def train_Data(train_data_list, taskId=taskId):
    input_train_file_name = taskId + "_input"
    input_lines = [[],[]]
    for data in train_data_list:
        ##单条的特征值
        attribute  = json.loads(data.similarity)
        ##是否相似商标
        is_similar = data.is_similar
        ##商标状态
        this_status = int(data.brand_sts)
        ##历史商标状态
        his_status = data.his_sts
        #if len(input_lines[this_status]) >= upb:
        #    continue
        class_no = data.class_no
        this_name = data.brand_name.replace(",",u"，")
        his_name = data.his_name.replace(",",u"，")
        #if (this_name,his_name) in name_pair:
        #    continue
        #name_pair.add((this_name,his_name))

        input_line = str(this_status)
        for i in range(len(attribute)):
            if math.isnan(attribute[i]):
                print "catch None"
                attribute[i] = 0.0
            input_line += " %d:%.2f"%(i, attribute[i])
        input_line += "\n"
        #if his_status == 1:
        input_lines[this_status].append( [input_line, ",".join([this_name, his_name, str(class_no)])] )

    print " 0 has %d, 1 has %d "%(len(input_lines[0]),len(input_lines[1]))

    train_lines = input_lines[0]
    train_lines.extend(input_lines[1])

    seed = 1
    np.random.seed(seed)
    np.random.shuffle(train_lines)
    print "train size %d"%(len(train_lines))

    save_input_file(train_lines,"../train/data/input/" + input_train_file_name)

def save_input_file(input_lines, input_file_name):
    with open(input_file_name, "w") as input_file:
        with open(input_file_name + "_content","w") as input_file_content:
            for input_line in input_lines:
                line = input_line[0]
                input_line_content = input_line[1]
                input_file_content.write(input_line_content + "\n")
                input_file.write(line)


"""脚本的命令行输入提示"""
def printUsage():
    print "usage: csvHandling.py -i <taskid> -f <configFileName>"

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
    task_sec = cf.options("task")
    train_parameters_sec = cf.options("train_parameter")

    task_dict = config_from_sec(task_sec, cf, "task")
    boost_parameters_dict = config_from_sec(boost_parameters_sec, cf, "boost_parameter")
    train_parameters_dict = config_from_sec(train_parameters_sec, cf, "train_parameter")

    return task_dict, boost_parameters_dict, train_parameters_dict, cf


def init_trans(taskId, configFile="train.config"):
    print u"任务id为%s" % (taskId)
    print "c=", configFile
    task_dict, boost_parameters_dict, train_parameters_dict, cf = load_config_file(configFile) #读取训练配置文件
    if task_dict["task_target"] == "train":
        train_Data(taskId, task_dict)

if __name__ == "__main__":
    """
        获取脚本执行参数：i 任务id （将作为此任务产生的所有文件的前缀）， f 配置文件（训练项目的配置）
        参数为可选，不输入时默认
        id= 运行时的时间戳
        f= 目录下的train.config文件
        """
    try:
        opts, args = getopt.getopt(sys.argv[1:], "i:f:", ["id=", "file="])
    except getopt.GetoptError:
        # 参数错误
        printUsage()
        sys.exit(-1)

    for opt, arg in opts:
        if opt in ("-i", "--id"):
            taskId = arg
        elif opt in ("-f", "--file"):
            configFile = arg

    init_trans(taskId=taskId, configFile=configFile)
    # load_brand_item()