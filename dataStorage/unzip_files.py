#-*-coding:utf8-*-
import time
import traceback
import pandas as pd
import sys
import os
reload(sys)
sys.setdefaultencoding("utf-8")
sys.path.append('..')
from consoleLogger import logger

class UnzipCarrier:

    def __init__(self, saving_dir="unzip_dir", dir_name="."):
        self.saving_dir = saving_dir
        #读取要转储的压缩包文件名
        if not os.path.exists(saving_dir): # 保存的目录不存在，创建一个
            os.system("mkdir %s" % saving_dir)
        filenames = self.get_file_names(dir_name)
        total_file = len(filenames)
        cnt_process = {True: 0, False: 0}
        for idx in range(total_file):
            filename = filenames[idx]
            process_res = self.unzip_file(filename)
            cnt_process[process_res] += 1
            if not idx%(total_file/10):
                logger.info(u"处理进度 %d/%d, 处理成功%d个" % (idx, total_file, cnt_process[True]))
        logger.info(u"全部压缩包解压完毕， 总计%d, 其中成功%d个，失败%d个" % (total_file, cnt_process[True], cnt_process[False]))

    def get_file_names(self, dir_name):
        u""" 返回指定目录下的所有zip后缀的文件名（带目录路径） """
        data_file_names = []
        for dir, subdir, files in os.walk(dir_name):
            for file in files:
                if file.find(".zip") != -1:
                    data_file_names.append(dir + "/" + file)
            return data_file_names

    ####record表存到redis中
    def unzip_file(self, zip_file_name):
        try:
            unzip_dir_name = zip_file_name.split(".zip")[0].replace(" ", "")
            os.system("unzip -o -z '%s'  -d  '%s/%s'" % (zip_file_name.encode("utf8"), self.saving_dir, unzip_dir_name.encode("utf8")))
            return True
        except:
            logger.error(u"解压压缩包%s时出现错误" % zip_file_name)
            return False


##975418个不同的商标，12277622
if __name__=="__main__":
    UnzipCarrier()







