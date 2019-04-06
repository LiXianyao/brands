#-*-coding:utf8-*-#
import pandas as pd
import codecs
import sys
import os
sys.path.append('..')
from consoleLogger import logger
import traceback

u"""
分别读取两种csv文件，对调试时遇到的可能的数据错误进行处理
"""
class CsvReader:
    def __init__(self):
        pass

    def load_csv_to_pandas(self, csv_name):
        res_state = True
        process_res = None
        try:
            clean_csv_name = self.process_csv_data(csv_name.encode("utf8"))
            new_csv_name = self.reStoreCsv(clean_csv_name)
            data = pd.read_csv(new_csv_name, encoding="utf-8", quotechar=None, quoting=3, dtype=str)
            rows, columns = data.shape
            logger.info(u"csv文件%s解析读取完成,共有%d行，%d列"%(csv_name, rows, columns))
            process_res = data
        except:
            res_state = False
            process_res = u"文件解析错误，请检查csv文件的正确性、完整性（例如将其另存为utf8格式等）"
            logger.error(process_res)
            logger.error(traceback.format_exc())
        return res_state, process_res


    def process_csv_data(self, csv_name):
        u"""
        对输入的csv文件进行处理，删除可能影响解析的因素，包括:
        编码异常
        去掉空白行：
        去掉\r\n:
        去掉\r：
         cat  注册商标基本信息.csv| sed ':a;N;$!ba;s/\r\n//g' | sed 's/\r//g'  > check.csv
        """
        clean_name = csv_name.replace(".csv", ".rm")
        command = "cat " + csv_name + "| sed ':a;N;$!ba;s/\\r\\n//g' | sed 's/\\r//g'  >" + clean_name
        os.system(command)
        logger.info(u"字符\\r与\\r\\n消除完毕！")
        return clean_name

    def reStoreCsv(self, csv_name):
        u"""
        对输入的csv文件进行处理，删除可能影响解析的因素，包括:
        编码异常
        去掉空白行：
        去掉\r\n:
        去掉\r：
         cat  注册商标基本信息.csv| sed ':a;N;$!ba;s/\r\n//g' | sed 's/\r//g'  > check.csv
        """
        f_old = open(csv_name, "rU")      #读取原本的数据（逐行）
        #new_csv_name = type + "_reStore.csv"
        new_csv_name = csv_name.replace(".rm", ".csv")
        f_new = codecs.open(new_csv_name, "w", "UTF-8")  #转存为UTF8格式
        cnt = 0
        cnt_error = 0
        cnt_blank = 0
        save_lines = []
        concatenate = False
        while True:
            cnt += 1
            try:  ##尝试读取数据行
                line = f_old.readline()
            except:
                cnt_error += 1
                logger.error(u"读取原始文件时转码错误，原因：存在不可解读为UTF-8的字符，已忽略此行数据,错误发生行号%d"%cnt)
                concatenate = False
                continue

            if not line:
                break ##文件读完跳出
            if len(line) == 1:
                concatenate = True
                cnt_blank += 1
                continue    #跳过空行

            last = line
            try:  ##尝试转存数据行
                if concatenate == True:
                    save_lines[-1] += line.decode("utf8")
                else:
                    save_lines.append(line.decode("utf8"))
            except:
                cnt_error += 1
                logger.error(u"保存数据行时转码错误，原因：存在不可保存为UTF-8的字符,已忽略此行数据,错误发生行号%d" % cnt)
            concatenate = False

        f_new.writelines(save_lines)
        logger.info(u"文件转码存储完成,原始文件共有%d行，其中转存出错%d行, 空白行%d行"%(cnt - 1, cnt_error, cnt_blank))
        f_old.close()
        f_new.close()
        os.system("rm " + csv_name)
        return new_csv_name

if __name__=="__main__":
    ##u"注册商标商品服务信息.csv"
    reader = CsvReader()
    print reader.load_csv_to_pandas(csv_name=u"注册商标基本信息.csv")
    print reader.load_csv_to_pandas(csv_name=u"注册商标商品服务信息.csv")
