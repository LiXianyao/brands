# -*- coding:utf-8 -*-#
import sys
sys.path.append("..")
from flask import request, Flask, make_response
import json, redis
import ConfigParser
import datetime, time, os, threading
import traceback
from train import form_pre_data_V_flask
from RetrievalResponse import BrandSimilarRetrievalResponse, RetrievalResponse
import check_data
from logger import flask_logger
from dataStorage.storage_connection import RedisConnection

"""##########################################由进程池子进程执行的函数们########################################################"""
###子进程重载分析函数的模块
def reload_module(id):
    reload(form_pre_data_V_flask)
    time.sleep(reload_sleep_time)
    flask_logger.info("process %d reload success!"%os.getpid())

###子进程关闭进程池前清除临时文件的模块
def remove_file(id):
    time.sleep(reload_sleep_time)
    input_file_name = str(os.getpid()) + "_input"
    os.system("rm " + "data/" + input_file_name)
    flask_logger.info("process %d remove input file success!" % os.getpid())

"""单个进程处理响应请求"""
def get_request(process_id, process_share_dict, input_json, item_dict = None):
    input_json["categories"] = input_json["categories"][process_id]
    reload(form_pre_data_V_flask)
    ###redis连接
    connection = RedisConnection()
    fix_con = connection.db
    _pipe = connection.pipe
    ##调用函数
    try:
        error_occur, query_res = form_pre_data_V_flask.form_pre_data_flask(input_json, item_dict, fix_con, _pipe, flask_logger)
        for class_no in input_json["categories"]:
            process_share_dict[class_no] = query_res[class_no]
    except:
        error_occur = True
        flask_logger.error("调用检索函数时发生异常!!", exc_info = True)

    if error_occur == True:
        process_share_dict["errorCode"] = "0"
        process_share_dict["message"] = "调用检索函数时发生异常!!请检查运行记录"
    print "process %d end"%process_id

"""##########################################服务进程的全局变量们########################################################"""
"""Flask配置"""
app = Flask(__name__)
app.config.from_object('flask_config')
setup_flag = False

"""redis模板库的配置"""
cf = ConfigParser.ConfigParser()
cf.read("redis.config")
redis_ip = cf.get("redis", "redis_ip")
redis_port = cf.get("redis", "redis_port")
redis_db = cf.get("redis", "redis_db")
redis_pwd = cf.get("redis", "redis_pwd")

####并发多进程配置的参数
total_process_num = int(cf.get("multiProcess","total_process_num"))
coreItem_process_num = int(cf.get("multiProcess","coreItem_process_num"))
restItem_process_num = int(cf.get("multiProcess","restItem_process_num"))
data_per_process = int(cf.get("multiProcess","data_per_process"))

try:
    from multiprocessing import Pool, Manager

    flask_logger.INFO(u"服务启动中... ...主进程号%s" % os.getpid())
    processManager = Manager()
    item_dict = form_pre_data_V_flask.load_brand_item()
    flask_logger.INFO(u"==========》》进程%d 读取小项列表完成!  下一项：创建进程池！《《=================" % (os.getpid()))
    processPool = Pool(total_process_num)
    flask_logger.INFO(u"==========》》进程%d 进程池创建完成!  服务进程初始化完成！！《《=================" % (os.getpid()))
except:
    flask_logger.error("进程池初始化失败！！！！", exc_info=True)
reload_sleep_time = 3
setup_flag = True

"""##########################################服务进程的接口定义########################################################"""
"""########################
核心类别检索的请求接口
主要特点是分配的进程数不同
"""###########################
@app.route('/api/retrieval/coreItem', methods=['POST'])
def retriCoreItem():
    print "retrieval/coreItem : ==============================>>>"
    input_json = request.json

    check, res = check_data.check_request_json(input_json)  ###检查json数据是否满足定义的实体类
    if check == True: ###
        global coreItem_process_num
        res = predict(input_json, process_num = coreItem_process_num)
    return form_response(res)

@app.route('/api/retrieval/restItem', methods=['POST'])
def retriRestItem():
    print "retrieval/restItem : ==============================>>>"
    input_json = request.json

    check, res = check_data.check_request_json(input_json)  ###检查json数据是否满足定义的实体类
    if check == True: ###
        global restItem_process_num
        res = predict(input_json, process_num = restItem_process_num)
    return form_response(res)

"""########################
调用检索模块进行近似商标检索。
根据不同业务配置的进程数和实际的类别数动态决定实际进程数。进行计算
"""###########################
def predict(input_json, process_num):
    global processManager, processPool, item_dict, data_per_process
    ###异常特判
    check, res = check_data.check_processpool(processPool, processManager, input_json)
    if check == False: ###服务进程初始化未完毕或异常，直接返回
        return res

    try:
        start_time_c = datetime.datetime.now()
        process_share = processManager.dict()
        running_process = []
        execute_process_num = check_data.divided_categories(process_num, input_json, data_per_process)  ##划分查询的类别
        flask_logger.info("new process num  =%d, divided categories =%s"%(execute_process_num, str(input_json["categories"])) )
        for i in range(execute_process_num):
            running_process.append(processPool.apply_async(get_request, args=(i, process_share, input_json, item_dict)))

        ##阻塞等待所有进程执行完毕
        for process in running_process:
            process.wait()
            print "process end"
        end_time_c = datetime.datetime.now()
        cost_time_c = (end_time_c - start_time_c).total_seconds()
        flask_logger.info(u"查询总耗时为 %s秒"%str(cost_time_c) )

        ##将结果封装到实体类，转化为Json字符串
        process_share_dict = dict(process_share)
        if process_share_dict.has_key("errorCode") and process_share_dict["errorCode"] == "0":  ###子进程执行期间抛出异常
            responseEntity = BrandSimilarRetrievalResponse(brandName=input_json["name"],
                                                           retrievalResult=[], resultCode="0",
                                                           message=process_share_dict["message"])
        else:
            responseEntity = BrandSimilarRetrievalResponse(brandName= input_json["name"], retrievalResult= process_share_dict.values(), resultCode="1", message="")
        process_share.clear()
        process_share_dict.clear()
        del running_process[:]
    except Exception, e:
        failed_Res = traceback.format_exc()
        flask_logger.error("调用多进程处理时发生异常!!", exc_info=True)
        # 把抛出的异常返回回去
        responseEntity = BrandSimilarRetrievalResponse(brandName=input_json["name"],
                                                       retrievalResult=[],
                                                       resultCode="0",
                                                       message=failed_Res)
    input_json.clear()
    return responseEntity

@app.route('/reload/prediction', methods=['POST'])
def reloadPrediction():
    try:
        global processPool
        reload(form_pre_data_V_flask)
        running_process = []
        for i in range(total_process_num):
            running_process.append(
                processPool.apply_async(reload_module, args=(i,)))
        for process in running_process:
            process.wait()
        flask_logger.info("prediction 模块重置成功！,pid=%d"%(os.getpid()) )
        response = RetrievalResponse(resultCode="1", message="prediction 模块重置成功！,pid=%d"%(os.getpid()))
    except:
        flask_logger.error("重置prediction模块时发生异常!!", exc_info=True)
        response = RetrievalResponse(resultCode= "0", message= "prediction 模块重置失败！！检查日志,pid=%d"%(os.getpid()))
    return form_response(response)

@app.route('/shutdown', methods=['POST'])
def shutdownProcess():
    try:
        global processPool, processManager
        if processPool == None:
            response = RetrievalResponse(resultCode="0", message="子进程未启动！,pid=%d" % (os.getpid()))
        else:###子进程已启动
            for i in range(total_process_num):
                processPool.apply_async(remove_file, args=(i,)) #删除运行时临时文件
            processPool.close()#关闭进程池
            processPool.join()
            del processManager
            del processPool
            processManager = None
            processPool = None
            flask_logger.info("==============>>子进程池关闭成功，可以直接结束服务进程,pid = %d<<========================="%(os.getpid()))
            response = RetrievalResponse(resultCode="1", message="子进程池关闭成功，可以直接结束服务进程！,pid=%d"%(os.getpid()) )
    except:
        flask_logger.error("关闭子进程池时发生异常!!", exc_info=True)
        response = RetrievalResponse(resultCode= "0", message= "子进程池关闭失败！！检查日志,pid=%d"%(os.getpid()) )
    return form_response(response)

##更新多进程配置参数
###只会改变执行时，每次请求使用多少进程，每个进程处理多少数据，改变进程数需要另外的
@app.route('/updateConfigure', methods=['POST'])
def updateConfigure():
    try:
        loadConfiguration()
        response_msg  =  "配置参数重置成功！！！pid=%d"%(os.getpid())
        flask_logger.info("配置参数重置成功！！！pid=%d"%(os.getpid()))
        response = RetrievalResponse(resultCode= "1", message=response_msg)
    except:
        flask_logger.error("更新配置参数时发生异常!! pid=%d"%(os.getpid()), exc_info=True)
        response = RetrievalResponse(resultCode= "0", message= traceback.format_exc())
    return form_response(response)


##测试接口的可用性
###主要是测试初始化进程池是否完毕
@app.route('/testUsage', methods=['POST'])
def testUsage():
    global setup_flag, processPool, processManager
    if setup_flag == False:##关闭原来的进程池
        response_msg = "服务进程初始化未结束，请稍作等待！！,pid=%d" % (os.getpid())
        flask_logger.info(response_msg)
        response = RetrievalResponse(resultCode="0", message=response_msg)
    elif processPool == None or processManager == None:
        response_msg = "服务进程初始化已结束，但进程池未启动，请尝试restProcessPool接口重置，或停止服务检查日志！！,pid=%d" % (os.getpid())
        flask_logger.error( response_msg )
        response = RetrievalResponse(resultCode= "0", message=response_msg)
    else:
        response_msg = "服务进程初始化正常结束，可以发送请求！,pid=%d" % (os.getpid())
        flask_logger.error(response_msg)
        response = RetrievalResponse(resultCode="1", message=response_msg)
    return form_response(response)

@app.route('/resetProcessPool', methods=['POST'])
def resetProcessPool():
    try:
        loadConfiguration()  ##重新载入配置
        global total_process_num, processPool, processManager
        if processPool!= None:##关闭原来的进程池
            processPool.close()
            processPool.join()
        from multiprocessing import Pool, Manager
        processPool = Pool(total_process_num)
        if processManager == None:
            processManager = Manager()
        response_msg  =  "服务进程池重置成功！！,pid=%d"%(os.getpid())
        flask_logger.info( "服务进程池重置成功！！,pid=%d"%(os.getpid()) )
        response = RetrievalResponse(resultCode= "1", message=response_msg)
    except:
        flask_logger.error("重置进程的子进程池时发生异常!!", exc_info=True)
        response = RetrievalResponse(resultCode= "0", message= traceback.format_exc())
    return form_response(response)

###响应实体类转为服务的json响应
def form_response(responseEntity):
    try:
        res = json.dumps(responseEntity, default=lambda obj: obj.__dict__, sort_keys=True,
                         ensure_ascii=False)
    except:
        flask_logger.error("封装响应实体类时发生异常!!", exc_info=True)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp
#setup()
#threading.Timer(5, setup).start()

def loadConfiguration():
    global cf, total_process_num, data_per_process, coreItem_process_num, restItem_process_num
    cf.read("redis.config")
    total_process_num = int(cf.get("multiProcess", "total_process_num"))
    coreItem_process_num = int(cf.get("multiProcess", "coreItem_process_num"))
    restItem_process_num = int(cf.get("multiProcess", "restItem_process_num"))
    data_per_process = int(cf.get("multiProcess", "data_per_process"))
    flask_logger.info(u"配置文件更新，总进程数=%d, 核心任务进程数=%d，非核心任务进程数=%d，进程最小处理类别数=%d"%(total_process_num, coreItem_process_num, restItem_process_num, data_per_process))

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
