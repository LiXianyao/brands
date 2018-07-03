# -*- coding:utf-8 -*-#
import sys
sys.path.append("..")
from flask import request, Flask, make_response
import json
import os
import ConfigParser
import redis
import time
import threading
import traceback
from train import form_pre_data_IV_flask

file_prefix = "./models/"
pid = str( os.getpid() )
now_Day = time.strftime("%d",time.localtime())

"""
连接数据库
"""
cf = ConfigParser.ConfigParser()
cf.read("redis.config")
cfLock = threading.Lock()

"""redis模板库的配置"""
fixed_ip = cf.get("redis", "fixed_ip")
fixed_port = cf.get("redis", "fixed_port")
fixed_db = cf.get("redis", "fixed_db")
fixed_pwd = cf.get("redis", "fixed_pwd")
fix_db = redis.StrictRedis(host=fixed_ip, port=fixed_port, db=fixed_db, password=fixed_pwd)  # 建立连接
fix_pipe = fix_db.pipeline()#

"""Flask配置"""
app = Flask(__name__)
app.config.from_object('flask_config')

"""任务使用的全局资源"""
record_id_dict = {}
record_time_dict = {}

"""只是一个手写的检查json里是否有可选字段的赋值函数"""
"""
    目前的可选字段是：1、额外信息 extraInformation：姓名、性别等的分析。
                     2、目标匹配标识 templateMatching。
                     3、黑词匹配标识 analysisFilter。
"""
def getOrDefault(input_json, segName, default):
    try:
        return input_json[segName]
    except:
        return default

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
        res = predict(input_json, AllClass = True, AllRes=False)
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
    global record_id_dict, record_time_dict
    return_list = form_pre_data_IV_flask.form_pre_data_flask(input_json, record_id_dict, record_time_dict, AllClass = AllClass,  AllRes = AllRes)
    print "总计有 %d 结果" % (len(return_list))
    input_json["searchRes"] = return_list
    try:
        pass
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
    reload(form_pre_data_IV_flask)
    res = {"message": "prediction 模块重置成功！"}
    res = json.dumps(res, ensure_ascii=False)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp

@app.route('/reload/train', methods=['POST'])
def reloadTrain():
    reload(form_pre_data_IV_flask)
    res = {"message": "train 模块重置成功！"}
    res = json.dumps(res, ensure_ascii=False)
    resp = make_response(res)
    resp.mimetype = 'application/json'
    return resp


"""初始化运行环境，调用读入词典、短信模板等初始化函数"""
def setup():
    global record_id_dict, record_time_dict
    db = redis.StrictRedis(host=fixed_ip, port=fixed_port, db=fixed_db, password=fixed_pwd)
    _pipe = db.pipeline()
    class_no_set = range(1, 46)
    record_key = "rd::"
    record_key_time = "rdt::"
    rset_key_prefix = "rset::"
    detail_key_prefix = "dtl::"
    ###获得所有的历史商标, 结果结构为 申请时间 -》 【商标名，商标编号，商标状态】
    record_id_dict, record_time_dict = form_pre_data_IV_flask.getHistoryBrand(record_key, db, class_no_set)
    print "history brand ready"

class NonASCIIJsonEncoder(json.JSONEncoder):
    def __init__(self, **kwargs):
        kwargs['ensure_ascii'] = False
        super(NonASCIIJsonEncoder, self).__init__(**kwargs)

app.json_encoder = NonASCIIJsonEncoder
setup()
if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5001)
