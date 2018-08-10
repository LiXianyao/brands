# -*- coding:utf-8 -*-#
import requests
import json
import datetime
import os, sys, getopt, csv
from BrandSimilarRetrievalRequest import BrandSimilarRetrievalRequest
reload(sys)
sys.setdefaultencoding( "utf-8" )

def send_request():
        brand_name = u"柠檬鱼"
        categories = range(1,46)
        requestsEntity = BrandSimilarRetrievalRequest(brandName=brand_name, categories=categories)
        r = requests.post("http://10.109.246.100:5000/api/retrieval/coreItem", json=requestsEntity.__dict__)
        return_msg = json.loads(r.text)
        print str(return_msg).replace('u\'', '\'').decode("unicode-escape")

if __name__ == "__main__":
    send_request()
