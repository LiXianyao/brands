# -*- coding:utf-8 -*-#
import sys
sys.path.append("..")
from flask import request, Flask, make_response
import json, redis
import ConfigParser
import datetime, time, os
import traceback
from train import form_pre_data_IV_flask

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
fix_con = redis.StrictRedis( host = redis_ip, port = redis_port, db = redis_db   , password = redis_pwd)

####并发多进程配置的参数
pool_num = int(cf.get("multiProcess","pool_num"))
process_num = int(cf.get("multiProcess","process_num"))
data_per_process = int(cf.get("multiProcess","data_per_process"))

processPoolSet = None
processManager = None

###全局数据结构
record_id_dict = []
record_key_dict = []
item_dict = {}

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
    global processManager, processPoolSet, process_num, item_dict
    try:
        start_time_c = datetime.datetime.now()
        process_share = processManager.dict()
        running_process = []
        for i in range(pool_num):
            processPool = processPoolSet[i]
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
        global processPoolSet, pool_num
        reload(form_pre_data_IV_flask)
        running_process = []
        for i in range(pool_num):
            processPool = processPoolSet[i]
            running_process.append(
                processPool.apply_async(reload_module, args=(i,)))
        for process in running_process:
            process.wait()
        res = {"message": "prediction 模块重置成功！"}
    except:
        print traceback.format_exc()
        res = {"message": "prediction 模块重置失败！！检查日志"}
    res = json.dumps(res, ensure_ascii=False)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

@app.route('/shutdown', methods=['POST'])
def shutdownProcess():
    try:
        global processPoolSet
        for processPool in processPoolSet:
            processPool.close()
            processPool.join()
        print "process end"
        res = {"message": "子进程池关闭成功，可以直接结束服务进程！"}
    except:
        print traceback.format_exc()
        res = {"message": "子进程池关闭失败！！检查日志"}
    res = json.dumps(res, ensure_ascii=False)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

def reload_module(id):
    reload(form_pre_data_IV_flask)
    time.sleep(2)
    print "process %d reload success!"%os.getpid()

def load_redis(class_no):
    record_key = "rd::"
    print "load redis of class %d"%(class_no)
    global data_per_process, record_id_dict , record_key_dict
    class_lowb = class_no * data_per_process + 1
    class_upb = min((class_no + 1) * data_per_process + 1, 46)
    class_no_set = range(class_lowb, class_upb)
    print class_no_set
    id_dict, key_dict = form_pre_data_IV_flask.getHistoryBrand(record_key, fix_con,class_no_set)
    record_id_dict.extend(id_dict)
    record_key_dict.extend(key_dict)
    print "process load class ",class_no_set, " end"


"""单个进程处理响应请求（先自检是否有数据-可能会有崩溃的进程被重启过）"""
def get_request(process_id, process_share_dict, input_json, item_dict = None, AllClass = False, AllRes = False):
    global  record_id_dict , record_key_dict
    if len(record_id_dict) == 0:
        print "class %d haven't loaded!" % process_id
        load_redis(process_id)
        print "class load complete! id dict size=%d"%(len(record_id_dict))
    if len(input_json) == 0:
        return

    ###本子进程要处理的数据范围
    class_lowb = process_id * data_per_process + 1
    class_upb = min((process_id + 1) * data_per_process + 1, 46)
    if AllClass == True:
        input_json["class"] = range(class_lowb, class_upb)
    else:
        input_json["class"] = set(input_json["class"]).intersection(set(range(class_lowb, class_upb)))
    query_res = form_pre_data_IV_flask.form_pre_data_flask(input_json, record_id_dict, record_key_dict, item_dict, AllClass = False, AllRes = AllRes)
    for class_no in input_json["class"]:
        process_share_dict[class_no] = query_res[class_no]
    #oput = str(dict(process_share_dict)).replace('u\'', '\'')
    #print oput.decode("unicode-escape")
    print "process %d end"%process_id



"""初始化运行环境，调用读入词典、短信模板等初始化函数"""
def setup():
    from multiprocessing import Pool, Manager
    global processManager, processPoolSet, process_num, item_dict, record_id_dict, record_key_dict, pool_num
    processManager = Manager()
    processPoolSet = []
    start_time_c = datetime.datetime.now()
    item_dict = form_pre_data_IV_flask.load_brand_item()
    running_process = []
    for i in range(pool_num):
        processPool = Pool(process_num)
        processPoolSet.append(processPool)
        for j in range(process_num):
            running_process.append(processPool.apply_async(load_redis, args=(i,)))


    for process in running_process:
        process.wait()
        # print "process end"
    end_time_c = datetime.datetime.now()
    cost_time_c = (end_time_c - start_time_c).total_seconds()
    print u"数据构造耗时为 :", cost_time_c
    print "history brand ready"

class NonASCIIJsonEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        kwargs['ensure_ascii'] = False
        super(NonASCIIJsonEncoder, self).__init__(**kwargs)

app.json_encoder = NonASCIIJsonEncoder

if __name__ == '__main__':
    setup()
    app.run(host='0.0.0.0', port=5001)
