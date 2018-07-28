#-*-coding:utf8-*-#
class BrandSimilarRetrievalResponse(object):
    def __init__(self, brandName, retrievalResult, resultCode, message):
        self.name = brandName
        self.retrievalResult = retrievalResult
        self.resultCode = resultCode
        self.message = message

    def __repr__(self):
        return str(self.__dict__)