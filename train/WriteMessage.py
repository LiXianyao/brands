#-*-coding:utf-8-*-#
'''
只是一个自己测试的时候用来给消息队列发json数据的代码
'''
import stomp
import json
import time
import sys
import os

class MyListener(object):
    def __init__(self):
        self.running = False
        self.endMission = True


    def on_error(self,headers,message):
        print('received an error %s'%message)
    def on_message(self,headers,message):
        print('received a message %s'%message)

conn = stomp.Connection10( [('127.0.0.1',61613)])
conn.set_listener('',MyListener())
conn.start()
conn.connect()

#从队列接收信息
conn.subscribe(destination='/queue/brandRes',id=1,ack='auto')

s_json = {
            #"type":"reload",
            #"type":"predict",
            "type":"shutdown",
            "text":
            {
            	"name":u"爱",
            	"class":[14],
            	"apply_date":u"2018年01月05日"

            }
}
s_json = json.dumps(s_json)
#向队列发送信息#
conn.send(body=s_json,destination='/queue/brand')
#json = "{\"taskId\":\"201708080001\",\"redisServerIp\":\"47.95.32.216\",\"redisServerPort\":\"9510\",\"redisDbId\":\"0\",\"smsCount\":\"4069717\"}"
#conn.send(body=json,destination='/queue/message')
