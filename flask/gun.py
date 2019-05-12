# -*- coding:utf-8 -*-#
import logging.handlers
from logging.handlers import WatchedFileHandler
import os,sys

debug = True
loglevel = 'debug'
logfile = 'log/debug.log'
bind = '0.0.0.0:5000'
pidfile = 'log/gunicorn.pid'
#loglevel = 'info'
timeout = 1200
access_log_format = '%(t)s %(p)s %(h)s "%(r)s" %(s)s %(L)s %(b)s %(f)s" "%(a)s"'
#设置gunicorn访问日志格式，错误日志无法设置
accesslog = "/dev/null"#访问日志文件的路径
#capture_output = True
#output_log_file = "/home/lab/brands/project/flask/log/output_error_log"
#os.system("rm " + output_log_file)
#errorlog = output_log_file#错误日志文件的路径
errlog = "/dev/null"

acclog = logging.getLogger('gunicorn.access')
acclog.addHandler(WatchedFileHandler('../flask/log/gunicorn_access.log'))
acclog.propagate = False
errlog = logging.getLogger('gunicorn.error')
errlog.addHandler(WatchedFileHandler('../flask/log/gunicorn_error.log'))
errlog.propagate = False

#启动的进程数
workers = 3