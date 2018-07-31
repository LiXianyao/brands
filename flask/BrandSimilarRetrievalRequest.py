#-*-coding:utf8-*-#
class BrandSimilarRetrievalRequest(object):
    def __init__(self, brandName, categories):
        self.name = brandName
        self.categories = categories
        for index in range(len(self.categories)):
            self.categories[index] = int(self.categories[index])

    def __repr__(self):
        return str(self.__dict__)