# -*- coding:utf-8 -*-#
import requests
import json
import datetime
import os, sys, getopt, csv
from BrandSimilarRetrievalRequest import BrandSimilarRetrievalRequest
reload(sys)
sys.setdefaultencoding( "utf-8" )

def send_request(file_name):
    ###检查文件是否存在
    if os.path.exists("test_input/" + file_name) == False:
        print "error!!商标文件应存放在test_input/目录下，仅输入文件名即可"
        exit(0)
    time_record_name = file_name.split(".")[0] + "_time_cost.txt"
    with open("test_input/" + file_name, "r") as f:
        with open("test_input/test_time_record/" + time_record_name,"w") as t_record:
            writer = csv.writer(t_record)
            lines = f.readlines()
            print u"=========>>开始请求，总计有商标名%d个(查询一个是几秒，把结果写文件要十几秒，推荐Nohup挂机)<<===================="%( len(lines) )
            cnt_line = 0
            for brand_name in lines:
                brand_name = brand_name.strip()
                requestsEntity = BrandSimilarRetrievalRequest(brandName=brand_name, categories=range(1,46))
                #print requestsEntity.__dict__,type(requestsEntity.__dict__)
                s_time_c = datetime.datetime.now()
                r = requests.post("http://10.109.246.100:5000/api/retrieval/coreItem", json=requestsEntity.__dict__)
                #return_msg = json.loads(r.text)
                e_time_c = datetime.datetime.now()
                cost_time_c = (e_time_c - s_time_c).total_seconds()
                cnt_line += 1
                print u"=====》》请求%d ,名字=“%s”查询耗时为 :%s秒"%(cnt_line, brand_name, str(cost_time_c))
                writer.writerow([brand_name, cost_time_c])

                return_msg = r.text
                with open("test_input/test_result/" + brand_name + ".res", "w") as req_res:
                    req_res.write(return_msg)


"""脚本的命令行输入提示"""
def printUsage():
    print "usage: test_file.py [-f <input text file route)>]"
    print u"-f <input text file route)> 可选参数，参数内容是作为输入的商标文件的文件名（相对路径，应放在test_input/目录下，仅输入文件名即可），默认为brand_name.txt"


if __name__ == "__main__":
    ##命令行参数获取输入文件名
    try:
        opts, args = getopt.getopt(sys.argv[1:], "f:", ["file="])
    except getopt.GetoptError:
        # 参数错误
        printUsage()
        sys.exit(-1)

    file_name = "brand_name.txt" ##默认文件名
    for opt,arg in opts:
        if opt in ("-f","--file"):
            file_name = arg

    send_request(file_name)