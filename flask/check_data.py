# -*- coding:utf-8 -*-#
from BrandSimilarRetrievalRequest import BrandSimilarRetrievalRequest
from RetrievalResponse import BrandSimilarRetrievalResponse
import json, ConfigParser, os


########检查进程池是否正常初始化#########
def check_processpool(processPool, processManager, input_json):
    if processPool == None or processManager == None:
        responseEntity = BrandSimilarRetrievalResponse(brandName=input_json["name"],
                                                       retrievalResult=[],
                                                       resultCode="0",
                                                       message="子进程pid=%d的进程池未初始化！请尝试通过/restProcessPool接口重置，或者重启服务"%os.getpid())
        res = json.dumps(responseEntity, default=lambda obj: obj.__dict__, sort_keys=True,
                         ensure_ascii=False)  # 将结果封装为json
        return False, res
    ###初始化 ok
    return True, ""


###############检查json数据是否满足定义的实体类########
def check_request_json(input_json):
    if len(input_json.keys()) == 0:  ###请求的json数据为空
        responseEntity = BrandSimilarRetrievalResponse(brandName=u"未解析成功",
                                                       retrievalResult=[],
                                                       resultCode="0",
                                                       message=u"请求数据格式有误（不是json数据）！")
        res = json.dumps(responseEntity, default=lambda obj: obj.__dict__, sort_keys=True,
                         ensure_ascii=False)  # 将结果封装为json
        return False, res
    else:  #检查请求的格式是否满足实体类定义
        try:
            requestEntity = BrandSimilarRetrievalRequest(input_json["name"], input_json["categories"])
            return True, ""
        except:
            responseEntity = BrandSimilarRetrievalResponse(brandName=u"未解析成功",
                                                           retrievalResult=[],
                                                           resultCode="0",
                                                           message=u"请求数据格式有误（json数据转化为实体类失败）！")
            res = json.dumps(responseEntity, default=lambda obj: obj.__dict__, sort_keys=True,
                             ensure_ascii=False)  # 将结果封装为json
            return False, res


########分割请求的大类列表，确定合适的分配进程数
def divided_categories(process_num, input_json, data_per_process):
    categories = input_json["categories"]
    categ_len = len(categories)
    ##类别太少 -》 减进程数； 类别足够=》直接划分

    cf = ConfigParser.ConfigParser()
    cf.read("redis.config")
    data_per_process = int(cf.get("multiProcess", "data_per_process"))

    min_data = data_per_process
    divided_categ = []
    if categ_len/min_data < process_num: ##进程数偏多
        min_data += 1 ##去尾，每个进程多处理一个类别的数据
        new_process_num = categ_len/min_data
        for i in range( new_process_num ):
            divided_categ.append(categories[ i * min_data: (i + 1) * min_data])
        if categ_len % min_data != 0:
            divided_categ.append(categories[new_process_num * min_data: ])
            new_process_num += 1
    else: ####类别足够，直接划分
        process_data = categ_len / process_num
        new_process_num = process_num
        plus_1 =  categ_len % process_num
        for i in range(process_num):
            divided_categ.append(categories[ i * process_data : (i + 1) * process_data ] )
        for i in range(plus_1):
            divided_categ[i].append(categories[ process_num * process_data + i ] )

    input_json["categories"] = divided_categ
    return new_process_num