#-*-coding:utf-8-*-#
import stomp
import time
import sys
import os
import form_pre_data_IV_flask
from functools import partial
import threading
import json
import traceback
import redis
import ConfigParser
import datetime


'''###################################
读取配置文件
'''###################################
cf = ConfigParser.ConfigParser()
cf.read("redis.config")
semaphoreNum = cf.get("MQ","semaphore_size")
fixed_ip = cf.get("redis","fixed_ip")
fixed_port = cf.get("redis","fixed_port")
fixed_db = cf.get("redis","fixed_db")
fixed_pwd = cf.get("redis","fixed_pwd")
default_ip = cf.get("redis","default_ip")
default_port = cf.get("redis","default_port")
default_db = cf.get("redis","default_db")
default_pwd = cf.get("redis","default_pwd")
fix_con = redis.StrictRedis( host = fixed_ip, port = fixed_port, db = fixed_db   , password = fixed_pwd)
#print fix_con.hgetall("tpl::msg::test1::1")

processPool = None
processManager = None
process_share = None
running = True
processNum = 9
data_per_process = 5

'''############################################################################
总入口，永久在线：
通过stomp侦听消息队列
    对收到的消息，满足要求格式时执行分析模块
    分析结束后向结果队列发送消息
    对不满足要求格式的消息，直接忽略（未解读出taskid，无法构造返回）

·多线程处理请求，对支持对多个请求并发执行
    使用信号量semaphore限制线程数量，线程数量上限threadNum = 4
'''############################################################################
class MyListener(object):
    def __init__(self):
        self.endMission = True
        self.semaphore = threading.Semaphore(int(semaphoreNum))
        self.lock = threading.Lock()
        self.thread = []
        self.running = {}


    def on_error(self,headers,message):
        print('received an error %s'%message)

    def runthread(self, input_json):
        global processManager, process_share, processPool, processNum
        #print processManager, process_share, processPool
        process_share = processManager.dict()
        start_time_c = datetime.datetime.now()
        processPool.map(partial(get_request, process_share_dict=process_share, input_json=input_json), range(processNum))
        end_time_c = datetime.datetime.now()
        cost_time_c = (end_time_c - start_time_c).total_seconds()
        print "查询耗时为 :",cost_time_c
        # processPool.map(partial(get_request, process_share_dict=process_share, input_json= input_json), range(1,6))

        process_share_dict = dict(process_share)
        #print process_share
        #print process_share_dict
        for class_no in process_share_dict:
            print "class_no = %d, has %s" % (
            class_no, str(process_share_dict[class_no]).replace('u\'', '\'').decode("unicode-escape"))


    def on_message(self,headers,message):
        print('received a message %s'%message)
        try:
            message = json.loads(message)
            print  message
        except Exception, e:
            res = {"taskId":"unknown", "resultCode":0, "resultMsg": traceback.format_exc()}
            returnMQ = json.dumps(res) #返回json
            #conn.send(body=returnMQ, destination='/queue/brandRes')
            print("loading error: %s" % traceback.format_exc())
            return
        #self.semaphore.acquire()
        try:
            if message['type'] == 'predict':
                input_json = message['text']
                Args =[input_json]
                threading.Thread(target = self.runthread, args = (Args)).start()
            elif message['type'] == 'shutdown':
                processPool.close()
                processPool.join()
                print "process end"
                global running
                running = False
                print "runnging = ",running
            elif message['type'] == 'reload':
                reload(form_pre_data_IV_flask)
                print "reload success!"

        except Exception, e:
            res = {"taskId":"unknown", "resultCode":0, "resultMsg": traceback.format_exc()}
            returnMQ = json.dumps(res) #返回json
            #conn.send(body=returnMQ, destination='/queue/brandRes')
            print("json error: %s" % traceback.format_exc())
            #解析存在问题，直接释放信号量
            #self.semaphore.release()
            print "wrong json"


conn = stomp.Connection10( [('127.0.0.1',61613)])
conn.set_listener('',MyListener())
conn.start()
conn.connect()

#从队列接收信息
conn.subscribe(destination='/queue/brand',id=1,ack='auto')

#向队列发送信息#
#conn.send(body='hello,garfield!',destination='/queue/test')

record_key = "rd::"
record_id_dict ={}
record_key_dict = {}

def load_redis(class_no):
    print "load redis of class %d"%(class_no)
    global record_id_dict, record_key_dict, data_per_process
    class_no_set = range(class_no * data_per_process + 1, (class_no + 1) * data_per_process)
    print class_no_set
    record_id_dict, record_key_dict = form_pre_data_IV_flask.getHistoryBrand(record_key, fix_con,class_no_set)

def get_request(process_id, process_share_dict, input_json):
    global  record_id_dict , record_key_dict
    if len(record_key_dict.keys()) == 0:
        print "class %d haven't loaded!" % process_id
        load_redis(process_id)
        #redis_cnt_dict[class_no] = record_key_dict[class_no]
    if len(input_json) == 0:
        return
    class_no_set = record_key_dict.keys()
    query_res = form_pre_data_IV_flask.form_pre_data_flask(input_json, record_id_dict, record_key_dict, AllClass = True, AllRes = True)
    for class_no in class_no_set:
        process_share_dict[class_no] = query_res[class_no]
    #oput = str(dict(process_share_dict)).replace('u\'', '\'')
    #print oput.decode("unicode-escape")
    print "process %d end"%process_id

def init_multiprocess():
    from multiprocessing import Pool, Manager
    global  processManager, process_share, processPool, processNum
    processManager = Manager()
    process_share = processManager.dict()
    processPool = Pool(processNum)
    input_json = {}
    start_time_c = datetime.datetime.now()
    processPool.map(partial(get_request, process_share_dict=process_share,input_json= input_json), range(processNum))
    #processPool.map(partial(get_request, process_share_dict=process_share, input_json= input_json), range(1,6))
    end_time_c = datetime.datetime.now()
    cost_time_c = (end_time_c - start_time_c).total_seconds()
    print "数据构造耗时为 :", cost_time_c
    print "init data finish!"
    process_share_dict = dict(process_share)
    print process_share
    print process_share_dict
    for class_no in process_share_dict:
        print "class_no = %d, has %s"%(class_no, str(process_share_dict[class_no]).replace('u\'', '\'').decode("unicode-escape"))

init_multiprocess()

while running == True:
    time.sleep(3)
print "bye~"