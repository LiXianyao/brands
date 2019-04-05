#-*-coding:utf8-*-#
import pandas as pd
import codecs
import os
import logging
logger = logging.getLogger(__name__)
logLevel = logging.DEBUG  ##正式运行的时候改成.INFO
logger.setLevel(level=logLevel)
console = logging.StreamHandler()
console.setLevel(logLevel)
logger.addHandler(console)

u"""
分别读取两种csv文件，对调试时遇到的可能的数据错误进行处理
"""
class CsvReader:
    def __init__(self):
        pass

    def load_csv_to_pandas(self, csv_name, type):
        new_csv_name = self.reStoreCsv(csv_name, type)
        #self.process_csv_data(new_csv_name)
        data = pd.read_csv(new_csv_name, encoding="utf-8", quotechar=None, quoting=3)
        print data.head()
        print data.shape

    def process_csv_data(self, csv_name):
        u"""
        对输入的csv文件进行处理，删除可能影响解析的因素，包括:
        编码异常
        去掉空白行：
        去掉\r\n:
        去掉\r：
         cat  注册商标基本信息.csv| sed ':a;N;$!ba;s/\r\n//g' | sed 's/\r//g'  > check.csv
        """
        command = "cat " + csv_name + "| sed ':a;N;$!ba;s/\\r\\n//g' | sed 's/\\r//g'  >" + csv_name
        #os.system(command)
        logger.info(u"字符\\r与\\n消除完毕！")

    def reStoreCsv(self, csv_name, type):
        u"""
        对输入的csv文件进行处理，删除可能影响解析的因素，包括:
        编码异常
        去掉空白行：
        去掉\r\n:
        去掉\r：
         cat  注册商标基本信息.csv| sed ':a;N;$!ba;s/\r\n//g' | sed 's/\r//g'  > check.csv
        """
        f_old = open(csv_name, "rU")      #读取原本的数据（逐行）
        new_csv_name = type + "_reStore.csv"
        f_new = codecs.open(new_csv_name, "w", "UTF-8")  #转存为UTF8格式
        cnt = 0
        cnt_error = 0
        cnt_blank = 0
        last = ""
        save_lines = []
        concate = False
        while True:
            cnt += 1
            try:  ##尝试读取数据行
                line = f_old.readline()
            except:
                cnt_error += 1
                logger.error(u"读取原始文件时转码错误，原因：存在不可解读为UTF-8的字符，已忽略此行数据,错误发生行号%d,最后一行有效数据为[%s]"%(cnt, last))
                concate = False
                continue

            if not line:
                break ##文件读完跳出
            if len(line) == 1:
                concate = True
                cnt_blank += 1
                continue    #跳过空行

            last = line
            try:  ##尝试转存数据行
                if concate == True:
                    save_lines[-1] += line.decode("utf8")
                else:
                    save_lines.append(line.decode("utf8"))
            except:
                logger.error(u"保存数据行时转码错误，原因：存在不可保存为UTF-8的字符,已忽略此行数据,错误发生行号%d,最后一行有效数据为" % cnt)
                print last
            concate = False

        f_new.writelines(save_lines)
        logger.info(u"文件转码存储完成,原始文件共有%d行，其中转存出错%d行, 空白行%d行"%(cnt - 1, cnt_error, cnt_blank))
        f_old.close()
        f_new.close()
        return new_csv_name

if __name__=="__main__":
    ##u"注册商标商品服务信息.csv"
    reader = CsvReader()
    reader.load_csv_to_pandas(csv_name=u"注册商标基本信息.csv",type="info")
