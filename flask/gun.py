# -*- coding:utf-8 -*-#
import gevent.monkey
gevent.monkey.patch_all()
import logging.handlers
from logging.handlers import WatchedFileHandler

import multiprocessing

debug = True
#loglevel = 'debug'
#logfile = 'log/debug.log'
bind = '0.0.0.0:5000'
pidfile = 'log/gunicorn.pid'
loglevel = 'info'
timeout = 300
access_log_format = '%(t)s %(p)s %(h)s "%(r)s" %(s)s %(L)s %(b)s %(f)s" "%(a)s"'
#设置gunicorn访问日志格式，错误日志无法设置
accesslog = "/dev/null"#访问日志文件的路径
errorlog = "/dev/null"#错误日志文件的路径

acclog = logging.getLogger('gunicorn.access')
acclog.addHandler(WatchedFileHandler('/home/lab/Documents/bussiness/online_server/log/gunicorn_access.log'))
acclog.propagate = False
errlog = logging.getLogger('gunicorn.error')
errlog.addHandler(WatchedFileHandler('/home/lab/Documents/bussiness/online_server/log/gunicorn_error.log'))
errlog.propagate = False

#启动的进程数
workers = multiprocessing.cpu_count() + 1
worker_class = 'gunicorn.workers.ggevent.GeventWorker'

x_forwarded_for_header = 'X-FORWARDED-FOR'