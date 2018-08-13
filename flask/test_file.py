# -*- coding:utf-8 -*-#
import requests
import json
import datetime
import os, sys, getopt, csv, traceback, threading
from BrandSimilarRetrievalRequest import BrandSimilarRetrievalRequest
from RetrievalResponse import BrandSimilarRetrievalResponse
reload(sys)
sys.setdefaultencoding( "utf-8" )
sys.path.append("..")
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
        if item_dict.has_key(item_name) == False:
            item_dict[item_name] = class_no
        else:
            if class_no != item_dict[item_name]:
                print "same name but different class! %s"%(item_name)
    del brand_item
    return item_dict

def locate_product(product, item_dict):
    class_list = set()
    not_in_data = set()
    for pro in product:
        pro = pro.strip()
        pro = pro.decode("utf8")
        pro = pro.replace("（","(").replace("）",")")
        try:
            class_no = item_dict[pro]
            class_list.add(class_no)
        except:
            #print traceback.format_exc()
            print "!!product %s not in the define!"%(pro)
            not_in_data.add(pro)
    return list(class_list), list(not_in_data)

def send_request(file_name, taskid):
    ###检查文件是否存在
    if os.path.exists("test_input/" + file_name) == False:
        print "error!!商标文件应存放在test_input/目录下，仅输入文件名即可"
        exit(0)
    time_record_name = "_time_cost.txt"
    item_dict = load_brand_item()
    threadlist = []
    with open("test_input/" + file_name, "r") as f:
        with open("test_input/test_time_record/" + taskid + time_record_name,"w") as t_record:
            writer = csv.writer(t_record)
            lines = f.readlines()
            print u"=========>>开始请求，总计有商标名%d个(查询一个是几秒，把结果写文件要十几秒，推荐Nohup挂机)<<===================="%( len(lines) )
            cnt_line = 0
            for line in lines:
                brand_name, product = line.split(",")
                brand_name = brand_name.strip()
                #product = product.split("，")
                #print product
                #categories, not_in_data = locate_product(product, item_dict)
                requestsEntity = BrandSimilarRetrievalRequest(brandName=brand_name, categories=range(1,46))
                #print requestsEntity.__dict__,type(requestsEntity.__dict__)
                s_time_c = datetime.datetime.now()
                r = requests.post("http://10.109.246.100:5000/api/retrieval/coreItem", json=requestsEntity.__dict__)
                #return_dict = json.loads(r.text)
                #countRes = countSimilarName(return_dict)
                e_time_c = datetime.datetime.now()
                cost_time_c = (e_time_c - s_time_c).total_seconds()
                cnt_line += 1
                print u"=====》》请求%d ,名字=“%s”查询耗时为 :%s秒" % (cnt_line, brand_name, str(cost_time_c))
                #if countRes["Sum"] > 0:
                    #print u"=====》》请求%d ,名字=“%s”查询类=%s 查询耗时为 :%s秒"%(cnt_line, brand_name, str(cost_time_c))
               # else:
                    #print u"!!!!!查0！！》》请求%d ,名字=“%s”查询类=%s 查询耗时为 :%s秒" % (cnt_line, brand_name, str(countRes), str(cost_time_c))
                #message = ""
                #if len(not_in_data) > 0:
                #    message = u"没有找到对应的商品项：%s"%(list(not_in_data))
                writer.writerow([cnt_line, brand_name, cost_time_c])

                return_msg = r.text
                threadlist.append(threading.Thread(target=writeFile, args=(cnt_line, return_msg)))
                threadlist[-1].start()

    print u"=====》》等待所有文件线程写完，请稍后……《《====="
    for thread in threadlist:
        thread.join()
    print u"=====》》测试完毕！！《《====="

def writeFile(brand_name, return_msg):
    with open("test_input/test_result/" + taskid + "_" + str(brand_name) + ".txt", "w") as req_res:
        req_res.write(return_msg)

def countSimilarName(return_dict):
    retrievalResponses = return_dict["retrievalResult"]
    countRes = {}
    countSum = 0
    for categoriRes in retrievalResponses:
        category = categoriRes["category"]
        similarName = len(categoriRes["similarName"])
        countRes.update({category : similarName})
        countSum += similarName
    #print countRes
    countRes["Sum"] = countSum
    return countRes


"""脚本的命令行输入提示"""
def printUsage():
    print "usage: test_file.py [-f <input text file route)>]"
    print u"-f <input text file route)> 可选参数，参数内容是作为输入的商标文件的文件名（相对路径，应放在test_input/目录下，仅输入文件名即可），默认为brand_name.txt"

if __name__ == "__main__":
    ##命令行参数获取输入文件名
    id = 1
    while os.path.exists("test_input/test_result/" + "test_505_" + str(id) + ".res") == True:
        os.system("cp test_input/test_result/test_505_" + str(id) + ".res test_input/test_result/test_505_" + str(id) + ".txt")
        os.system("rm test_input/test_result/test_505_" + str(id) + ".res")
        id += 1
        print id

    exit(0)
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:i:", ["file=","taskid="])
    except getopt.GetoptError:
        # 参数错误
        printUsage()
        sys.exit(-1)

    file_name = "brand_name.txt" ##默认文件名
    taskid = "test_default"
    for opt,arg in opts:
        if opt in ("-f","--file"):
            file_name = arg
        elif opt in ("-i","--taskid"):
            taskid = arg

    send_request(file_name, taskid)