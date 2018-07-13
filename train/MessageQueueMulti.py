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

redis_ip = cf.get("redis","redis_ip")
redis_port = cf.get("redis","redis_port")
redis_db = cf.get("redis","redis_db")
redis_pwd = cf.get("redis","redis_pwd")
fix_con = redis.StrictRedis( host = redis_ip, port = redis_port, db = redis_db   , password = redis_pwd)

####并发多进程配置的参数
pool_num = int(cf.get("multiProcess","pool_num"))
process_num = int(cf.get("multiProcess","process_num"))
data_per_process = int(cf.get("multiProcess","data_per_process"))

processPoolSet = None
processManager = None
running = True

###全局数据结构
record_id_dict = []
record_key_dict = []
item_dict = {}

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
        global processManager, processPoolSet, process_num, item_dict
        start_time_c = datetime.datetime.now()
        """
        query_res = form_pre_data_IV_flask.form_pre_data_flask(input_json, record_id_dict, record_key_dict, item_dict,
                                                               AllClass=True, AllRes=True)
        oput = str(dict(query_res)).replace('u\'', '\'')
        print oput.decode("unicode-escape")
        print "process end"
        """
        process_share = processManager.dict()
        running_process = []
        for i in range(pool_num):
            processPool = processPoolSet[i]
            running_process.append(processPool.apply_async(get_request, args=(i, process_share, input_json, item_dict)) )
        print "process running"
        for process in running_process:
            process.wait()
            print "process end"
        end_time_c = datetime.datetime.now()
        cost_time_c = (end_time_c - start_time_c).total_seconds()
        print u"查询总耗时为 :",cost_time_c
        # processPool.map(partial(get_request, process_share_dict=process_share, input_json= input_json), range(1,6))

        process_share_dict = dict(process_share)
        #print process_share
        #print process_share_dict
        for class_no in process_share_dict:
            print "class_no = %d, has %s" % (
            class_no, str(process_share_dict[class_no]).replace('u\'', '\'').decode("unicode-escape"))
        process_share.clear()
        process_share_dict.clear()
        del running_process[:]
        #"""


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
                global processPoolSet
                for processPool in processPoolSet:
                    processPool.close()
                    processPool.join()
                print "process end"
                global running
                running = False
                print "runnging = ",running
            elif message['type'] == 'reload':
                global processPoolSet, pool_num
                reload(form_pre_data_IV_flask)
                running_process = []
                for i in range(pool_num):
                    processPool = processPoolSet[i]
                    running_process.append(
                        processPool.apply_async(reload_module, args=(i, )))
                for process in running_process:
                    process.wait()
                print "reload success!"

        except Exception, e:
            res = {"taskId":"unknown", "resultCode":0, "resultMsg": traceback.format_exc()}
            returnMQ = json.dumps(res) #返回json
            #conn.send(body=returnMQ, destination='/queue/brandRes')
            print("json error: %s" % traceback.format_exc())
            #解析存在问题，直接释放信号量
            #self.semaphore.release()
            print "wrong json"

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

def get_request(process_id, process_share_dict, input_json, item_dict = None):
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
    input_json["class"] = range(class_lowb, class_upb)
    query_res = form_pre_data_IV_flask.form_pre_data_flask(input_json, record_id_dict, record_key_dict, item_dict, AllClass = False, AllRes = True)
    for class_no in input_json["class"]:
        process_share_dict[class_no] = query_res[class_no]
    #oput = str(dict(process_share_dict)).replace('u\'', '\'')
    #print oput.decode("unicode-escape")
    print "process %d end"%process_id

def init_multiprocess():
    from multiprocessing import Pool, Manager
    global  processManager, processPoolSet, process_num, item_dict, record_id_dict, record_key_dict, pool_num
    processManager = Manager()
    processPoolSet = []
    start_time_c = datetime.datetime.now()
    item_dict = form_pre_data_IV_flask.load_brand_item()
    running_process = []
    for i in range(pool_num):
        processPool = Pool(process_num)
        processPoolSet.append(processPool)
        for j in range(process_num):
            running_process.append(processPool.apply_async(load_redis, args=(i,)) )

    for process in running_process:
        process.wait()
        #print "process end"
    end_time_c = datetime.datetime.now()
    cost_time_c = (end_time_c - start_time_c).total_seconds()
    print u"数据构造耗时为 :", cost_time_c
    print "init data finish!"


if __name__=="__main__":
    conn = stomp.Connection10([('127.0.0.1', 61613)])
    conn.set_listener('', MyListener())
    conn.start()
    conn.connect()

    # 从队列接收信息
    conn.subscribe(destination='/queue/brand', id=1, ack='auto')
    init_multiprocess()

    while running == True:
        time.sleep(3)
    print "bye~"