#-*-coding:utf8-*-#
import sys
import  ConfigParser
import os,csv
import getopt
import time
import numpy as np
sys.path.append("..")
reload(sys)
sys.setdefaultencoding( "utf-8" )

def train_Data(taskId, task_dict, boost_parameters_dict, train_parameters_dict):
    row_len = 18
    csv_name = task_dict["train_set"]
    input_train_file_name = taskId + "_train"
    input_test_file_name = taskId + "_test"
    upb = 10
    name_pair = set()
    with open(csv_name, "rU") as csv_file:
        input_lines = {"0":[], "1":[]}
        reader = csv.reader(csv_file, delimiter=',', quotechar='\n')
        line_cnt = 0
        for line in reader:
            line_cnt += 1
            if line_cnt == 1:
                continue
            if len(line) < row_len:
                print "line %d error, as:"%(line_cnt)
                # print line
                continue

            ##单条的特征值
            attribute  = line[-13 : -3]
            ##是否相似商标
            is_similar = line[-1]
            ##商标状态
            this_status = line[-3]
            ##历史商标状态
            his_status = line[-2]
            #if len(input_lines[this_status]) >= upb:
            #    continue
            class_no = line[-14]
            this_brand_no = line[-15]
            his_brand_no = line[-16]
            this_name = line[0]
            his_name = ",".join(line[1:-16])
            #if (this_name,his_name) in name_pair:
            #    continue
            #name_pair.add((this_name,his_name))

            input_line = this_status
            for i in range(len(attribute)):
                attri = attribute[i]
                if attri == "nan":
                    print "catch None"
                    attri = str(0.0)
                input_line += " " + str(i) + ":" + attri
            input_line += "\n"
            if his_status == "1":
                input_lines[this_status].append((input_line,"&*(".join([this_name, his_name, class_no]) ) )

            #if len(input_lines["0"]) >= upb and len(input_lines["1"])>= upb:
            #   break

    ratio = task_dict["ratio"]
    print "ratio = %f , 0 has %d, 1 has %d "%(ratio,len(input_lines["0"]),len(input_lines["1"]))
    min_size = min(len(input_lines["0"]), len(input_lines["1"]))
    train_size = int(ratio * min_size)
    print "train_size = %d"%(train_size)

    seed = 10
    np.random.seed(seed)
    np.random.shuffle(input_lines["0"])
    np.random.shuffle(input_lines["1"])

    train_lines = input_lines["0"][:train_size]
    train_lines.extend(input_lines["1"][:train_size])

    test_lines = input_lines["0"][train_size:min_size]
    test_lines.extend(input_lines["1"][train_size:min_size])

    seed = 1
    np.random.seed(seed)
    np.random.shuffle(train_lines)
    np.random.shuffle(test_lines)
    print "train size %d, test size %d"%(len(train_lines), len(test_lines))

    save_input_file(train_lines,"data/input/" + input_train_file_name)
    save_input_file(test_lines, "data/input/" + input_test_file_name)


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

    """时间戳！！用于区分同一套模型/训练数据/测试数据"""
    nowtime = time.strftime("%Y%m%d%H%M", time.localtime())
    taskId = "default_train_" + nowtime
    configFile = "train.config"

    for opt, arg in opts:
        if opt in ("-i", "--id"):
            taskId = arg
            nowtime = arg
        elif opt in ("-f", "--file"):
            configFile = arg
    print "任务id为%s" % (nowtime)
    print "c=", configFile
    task_dict, boost_parameters_dict, train_parameters_dict, cf = load_config_file(configFile) #读取训练配置文件
    if task_dict["task_target"] == "train":
        train_Data(nowtime, task_dict, boost_parameters_dict, train_parameters_dict)
    # load_brand_item()