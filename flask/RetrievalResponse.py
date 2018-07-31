#-*-coding:utf8-*-#
###普通请求的应答实体
class RetrievalResponse(object):
    def __init__(self,resultCode, message):
        self.resultCode = resultCode
        self.message = message

    def __repr__(self):
        return str(self.__dict__)

###近似商标检索请求的response
class BrandSimilarRetrievalResponse(object):
    def __init__(self, brandName, retrievalResult, resultCode, message):
        self.name = brandName
        self.retrievalResult = retrievalResult
        self.resultCode = resultCode
        self.message = message

    def __repr__(self):
        return str(self.__dict__)