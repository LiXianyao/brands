#-*-coding:utf-8-*-#
import sys
import glob
sys.path.append('gen-py')
sys.path.append("..")
sys.path.insert(0, glob.glob('/home/lab/thrift-0.11.0/lib/py/build/lib*')[0])

from train import form_pre_data_IV_flask
from functools import partial
import threading
from brand_service import BrandSearch
from brand_service.ttypes import tuple, groupRes
import json
import traceback
import redis
import ConfigParser
import datetime


'''###################################
读取配置文件
'''###################################
cf = ConfigParser.ConfigParser()
cf.read("rpc_server.config")
###rpc服务器的参数
semaphoreNum = cf.get("RPC","semaphore_size")
rpc_ip =  cf.get("RPC","rpc_ip")
rpc_port  = cf.get("RPC","rpc_port")
###redis数据源的参数
redis_ip = cf.get("redis","redis_ip")
redis_port = cf.get("redis","redis_port")
redis_db = cf.get("redis","redis_db")
redis_pwd = cf.get("redis","redis_pwd")
fix_con = redis.StrictRedis( host = redis_ip, port = redis_port, db = redis_db   , password = redis_pwd)
####并发多进程配置的参数
process_num = int(cf.get("multiProcess","process_num"))
data_per_process = int(cf.get("multiProcess","data_per_process"))

###由通信主进程创建并管理的全局变量
processPool = None
processManager = None
process_share = None
server = None

##保存各个子进程读取redis的数据
record_id_dict = {}
record_key_dict = {}

'''############################################################################
RPC服务的实现类：
对客户端的远程调用做出响应，给出返回值
'''############################################################################
class  BrandSearchHandler():
    def __init__(self):
        self.semaphore = threading.Semaphore(int(semaphoreNum))
        self.lock = threading.Lock()

    def queryBrand(self, inputJson):
        global processManager, process_share, processPool, process_num
        #print processManager, process_share, processPool
        process_share = processManager.dict()
        start_time_c = datetime.datetime.now()
        processPool.map(partial(get_request, process_share_dict=process_share, input_json=inputJson), range(process_num))
        end_time_c = datetime.datetime.now()
        cost_time_c = (end_time_c - start_time_c).total_seconds()
        print "查询耗时为 :",cost_time_c
        # processPool.map(partial(get_request, process_share_dict=process_share, input_json= input_json), range(1,6))

        process_share_dict = dict(process_share)
        for class_no in process_share_dict:
            print "class_no = %d, has %s" % (
            class_no, str(process_share_dict[class_no]).replace('u\'', '\'').decode("unicode-escape"))
        return process_share_dict


    def stop(self):
        print "in stop"
        global serverm, processPool
        processPool.close()
        processPool.join()
        print "process end"
        server.stop()
        print "server = ", server

    def reload(self):
        print "in reload"
        reload(form_pre_data_IV_flask)
        return "module prediction reload sucess!"

def buildServer():
    from thrift.transport import TSocket, TTransport
    from thrift.server import TServer
    from thrift.protocol import TBinaryProtocol
    global rpc_ip, rpc_port, server, semaphoreNum

    service_handler = BrandSearchHandler()
    service_processor = BrandSearch.Processor(service_handler)
    tsocket = TSocket.TServerSocket(rpc_ip, rpc_port)
    trans_fac = TTransport.TBufferedTransportFactory()
    protol_fac = TBinaryProtocol.TBinaryProtocolFactory()
    server = TServer.TThreadPoolServer(service_processor, tsocket, trans_fac, protol_fac)
    server.setNumThreads(semaphoreNum)
    init_multiprocess()

    server.serve()
    print "server ready"

def load_redis(class_no):
    record_key = "rd::"
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
    global  processManager, process_share, processPool, process_num
    processManager = Manager()
    process_share = processManager.dict()
    processPool = Pool(process_num)
    input_json = {}
    start_time_c = datetime.datetime.now()
    processPool.map(partial(get_request, process_share_dict=process_share,input_json= input_json), range(process_num))
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