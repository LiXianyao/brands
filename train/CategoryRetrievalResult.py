#-*-coding:utf8-*-#
class CategoryRetrievalResult(object):
    def __init__(self, category, similarNameList, goodsRegisterRateList):
        self.category = category
        self.similarName = similarNameList
        self.goodsRegisterRate = goodsRegisterRateList

    def __repr__(self):
        return str(self.__dict__)

    def countSimilarName(self):
        return {self.category : len(self.similarName)}

    def getNameListLen(self):
        if self.similarName:
            return len(self.similarName)
        else:
            return 0

    def getCategory(self):
        return self.category

    def getInfo(self):
        return self.getCategory(), self.getNameListLen()