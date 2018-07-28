#-*-coding:utf8-*-#
class CategoryRetrievalResult(object):
    def __init__(self, category, similarNameList, goodsRegisterRateList):
        self.category = category
        self.similarName = similarNameList
        self.goodsRegisterRate = goodsRegisterRateList

    def __repr__(self):
        return str(self.__dict__)