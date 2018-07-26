# -*- coding:utf-8 -*-#
import sys
sys.path.append("..")
from flask import request, Flask, make_response
import json, redis
import ConfigParser
import datetime, time, os, threading
import traceback
from train import form_pre_data_V_flask

"""Flask配置"""
app = Flask(__name__)
app.config.from_object('flask_config')

"""任务使用的全局资源"""
"""redis模板库的配置"""
cf = ConfigParser.ConfigParser()
cf.read("redis.config")
redis_ip = cf.get("redis", "redis_ip")
redis_port = cf.get("redis", "redis_port")
redis_db = cf.get("redis", "redis_db")
redis_pwd = cf.get("redis", "redis_pwd")


"""单个进程处理响应请求"""
def get_request(process_id, process_share_dict, input_json, item_dict = None, AllClass = False, AllRes = False):
    ###本子进程要处理的数据范围
    class_lowb = process_id * data_per_process + 1
    class_upb = min((process_id + 1) * data_per_process + 1, 46)
    if AllClass == True:
        input_json["class"] = range(class_lowb, class_upb)
    else:
        input_json["class"] = set(input_json["class"]).intersection(set(range(class_lowb, class_upb)))
    reload(form_pre_data_V_flask)
    ###redis连接
    fix_con = redis.StrictRedis(host=redis_ip, port=redis_port, db=redis_db, password=redis_pwd)
    _pipe = fix_con.pipeline()
    ##调用函数
    query_res = form_pre_data_V_flask.form_pre_data_flask(input_json, item_dict, fix_con, _pipe, AllClass = False, AllRes = AllRes)
    for class_no in input_json["class"]:
        process_share_dict[class_no] = query_res[class_no]
    #oput = str(dict(process_share_dict)).replace('u\'', '\'')
    #print oput.decode("unicode-escape")
    print "process %d end"%process_id

####并发多进程配置的参数
process_num = int(cf.get("multiProcess","process_num"))
data_per_process = int(cf.get("multiProcess","data_per_process"))
from multiprocessing import Pool, Manager
print "setup process,", os.getpid()
processManager = Manager()
item_dict = form_pre_data_V_flask.load_brand_item()
print u"==========》》进程%d 读取小项列表完成!  下一项：创建进程池！《《=================" % (os.getpid())
processPool = Pool(process_num)
print u"==========》》进程%d 进程池创建完成!  服务进程初始化完成！！《《=================" % (os.getpid())
reload_sleep_time = 3

@app.route('/predict/allRes', methods=['POST'])
def predictAllRes():
    print "allRes : ==============================>>>"
    #print request.form
    #print request.json
    if request.method == 'POST':
        try:
            text = request.form['text']
            input_json = json.loads(text)
            #print input_json
        except:
            input_json = request.json['text']
            #print input_json

    try:
        res = predict(input_json, AllClass = False, AllRes=True)
        #print res, type(res)
    except:
        input_json["resultCode"] = "0"
        input_json["message"] = "错误的表单数据!"
        res =json.dumps(input_json, ensure_ascii=False)

    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

@app.route('/predictAll/onlyName', methods=['POST'])
def predictAllOnlyName():
    print "All onlyName : ==============================>>>"
    #print request.form
    #print request.json
    if request.method == 'POST':
        try:
            text = request.form['text']
            input_json = json.loads(text)
            #print input_json
        except:
            input_json = request.json['text']
            #print input_json

    try:
        res = predict(input_json, AllClass = True, AllRes=False)
        #print res, type(res)
    except:
        input_json["resultCode"] = "0"
        input_json["message"] = "错误的表单数据!"
        res =json.dumps(input_json, ensure_ascii=False)

    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

@app.route('/predictAll/allRes', methods=['POST'])
def predictAllAllRes():
    print "All allRes : ==============================>>>"
    #print request.form
    #print request.json
    if request.method == 'POST':
        try:
            text = request.form['text']
            input_json = json.loads(text)
            #print input_json
        except:
            input_json = request.json['text']
            #print input_json

    try:
        res = predict(input_json, AllClass = True, AllRes=True)
        #print res, type(res)
    except:
        input_json["resultCode"] = "0"
        input_json["message"] = "错误的表单数据!"
        res =json.dumps(input_json, ensure_ascii=False)

    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

@app.route('/predict/onlyName', methods=['POST'])
def predictOnlyName():
    print "onlyName : ==============================>>>"
    #print request.form
    #print request.json
    if request.method == 'POST':
        try:
            text = request.form['text']
            input_json = json.loads(text)
            #print input_json
        except:
            input_json = request.json['text']
            #print input_json

    try:
        res = predict(input_json, AllClass = False, AllRes=False)
    except:
        input_json["resultCode"] = "0"
        input_json["message"] = "错误的表单数据!"
        res =json.dumps(input_json, ensure_ascii=False)

    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

##使用预测
def predict(input_json, AllClass = False, AllRes = False):
    #print ">>>",AllClass, AllRes
    global processManager, processPool, process_num, item_dict
    try:
        start_time_c = datetime.datetime.now()
        process_share = processManager.dict()
        running_process = []
        for i in range(process_num):
            running_process.append(processPool.apply_async(get_request, args=(i, process_share, input_json, item_dict, AllClass, AllRes)))
        print "process running"
        for process in running_process:
            process.wait()
            print "process end"
        end_time_c = datetime.datetime.now()
        cost_time_c = (end_time_c - start_time_c).total_seconds()
        print u"查询总耗时为 :", cost_time_c

        process_share_dict = dict(process_share)
        input_json["searchRes"] = process_share_dict
        res = json.dumps(input_json, ensure_ascii=False)  # 将结果封装为json
        process_share.clear()
        process_share_dict.clear()
        del running_process[:]
    except Exception, e:
        failed_Res = traceback.format_exc()
        print"trace :%s" % (failed_Res)
        # 把这个e作为中间结果返回回去
        input_json["resultCode"] = "0"
        input_json["message"] = failed_Res
        res = json.dumps(input_json, ensure_ascii=False)  # 将结果封装为json
    input_json.clear()
    return res

@app.route('/reload/prediction', methods=['POST'])
def reloadPrediction():
    try:
        global processPool
        reload(form_pre_data_V_flask)
        running_process = []
        for i in range(process_num):
            running_process.append(
                processPool.apply_async(reload_module, args=(i,)))
        for process in running_process:
            process.wait()
        res = {"message": "prediction 模块重置成功！,pid=%d"%(os.getpid())}
    except:
        print traceback.format_exc()
        res = {"message": "prediction 模块重置失败！！检查日志,pid=%d"%(os.getpid())}
    res = json.dumps(res, ensure_ascii=False)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

@app.route('/shutdown', methods=['POST'])
def shutdownProcess():
    try:
        global processPool, processManager
        processPool.close()
        processPool.join()
        del processManager
        del processPool
        processManager = None
        processPool = None
        print "==============>>子进程池关闭成功，可以直接结束服务进程,pid = %d<<========================="%(os.getpid())
        res = {"message": "子进程池关闭成功，可以直接结束服务进程！,pid=%d"%(os.getpid())}
    except:
        print traceback.format_exc()
        res = {"message": "子进程池关闭失败！！检查日志,pid=%d"%(os.getpid())}
    res = json.dumps(res, ensure_ascii=False)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

###子进程重载分析函数的模块
def reload_module(id):
    reload(form_pre_data_V_flask)
    time.sleep(reload_sleep_time)
    print "process %d reload success!"%os.getpid()



"""初始化运行环境，调用读入词典、短信模板等初始化函数"""
def setup():
    from multiprocessing import Pool, Manager
    global processManager, processPool, process_num, item_dict, record_id_dict, record_key_dict
    print "setup process,",os.getpid()
    processManager = Manager()
    item_dict = form_pre_data_V_flask.load_brand_item()
    print u"==========》》进程%d 读取小项列表完成!  下一项：创建进程池！《《================="%( os.getpid() )
    processPool = Pool(process_num)
    print u"==========》》进程%d 进程池创建完成!  服务进程初始化完成！！《《================="%( os.getpid() )

###备用的启动接口
@app.route('/setup', methods=['POST'])
def webSetup():
    setup()
    res = {"message": "进程数据初始化成功！！"}
    res = json.dumps(res, ensure_ascii=False)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

##更新多进程配置参数
###只会改变执行时，每次请求使用多少进程，每个进程处理多少数据，改变进程数需要另外的
@app.route('/updateConfigure', methods=['POST'])
def updateConfigure():
    global cf, process_num, data_per_process
    cf.read("redis.config")
    process_num = int(cf.get("multiProcess", "process_num"))
    data_per_process = int(cf.get("multiProcess", "data_per_process"))
    res = {"message": "配置参数重置成功！！,pid=%d"%(os.getpid())}
    res = json.dumps(res, ensure_ascii=False)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp


##重置进程池
###根据配置文件的更新重置进程池
@app.route('/resetProcessPool', methods=['POST'])
def resetProcessPool():
    global cf, process_num, data_per_process, processPool
    cf.read("redis.config")
    process_num = int(cf.get("multiProcess", "process_num"))
    data_per_process = int(cf.get("multiProcess", "data_per_process"))
    processPool.close()
    processPool.join()
    from multiprocessing import Pool
    processPool = Pool(process_num)
    res = {"message": "服务进程池重置成功！！,pid=%d"%(os.getpid())}
    res = json.dumps(res, ensure_ascii=False)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

#setup()
#threading.Timer(5, setup).start()

class NonASCIIJsonEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        kwargs['ensure_ascii'] = False
        super(NonASCIIJsonEncoder, self).__init__(**kwargs)

app.json_encoder = NonASCIIJsonEncoder
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
