# -*- coding:utf-8 -*-#
import logging
import os
logger = logging.getLogger(__name__)
logLevel = logging.DEBUG  ##正式运行的时候改成.INFO
logger.setLevel(level=logLevel)
handler = logging.FileHandler("log/gunicorn_stdout.log")
handler.setLevel(logLevel)
formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s : %(message)s")
handler.setFormatter(formatter)

console = logging.StreamHandler()
console.setFormatter(formatter)
console.setLevel(logLevel)

logger.addHandler(handler)
logger.addHandler(console)

logger.info("service setup")