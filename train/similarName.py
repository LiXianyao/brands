#-*-coding:utf8-*-#
from similarity import convert
from similarity.compute import similar_gate_low, default_gate, search_gate

###各个特征属性的名字（按构造得到的特征数据的顺序）
attribute_title = [u"汉字编辑距离相似度", u"拼音相似度", u"汉字包含被包含",
                   u"汉字排列组合", u"汉字含义相近", u"汉字字形相似度",
                   u"英文编辑距离相似度", u"英文包含被包含",u"英文排列组合",
                   u"数字完全匹配"]

#这些特征属性依照甲方定义的比较顺序
orderedIndex = {
                    u"汉字包含被包含": 1,
                    u"汉字编辑距离相似度": 2,
                    u"汉字排列组合": 3,
                    u"拼音相似度": 4,
                    u"汉字含义相近": 5,
                    u"汉字字形相似度": 6,
                    u"英文编辑距离相似度": 7,
                    u"英文包含被包含": 8,
                    u"英文排列组合": 9,
                    u"数字完全匹配": 10
                }

def sortNames( (rate, title) ):##输入是一对元组
    global orderedIndex
    return orderedIndex[title]


####近似名字对象
class similarName:
    def __init__(self, compareName, name, register_no, attriList, predict_rate):
        self.name = name
        self.register_no = register_no
        self.calMaxAttri(attriList)
        self.predict_rate = predict_rate
        self.convertTag(compareName)


    ###确定优先级最高的特征类型
    def calMaxAttri(self, attriList):
        attriList = zip(attriList, attribute_title)
        attriList.sort(key = sortNames)
        titledGate = zip(default_gate, attribute_title)
        titledGate.sort(key = sortNames)
        (self.rate, self.rateTitle) = search_gate(attriList, titledGate)

        if (self.rate, self.rateTitle) == (None, None):
            (self.rate, self.rateTitle) = max(attriList, key=lambda x: x[0])
        self.rate = round(self.rate * 100.0, 2) ##转换为百分数，两位小数

    ##转化特征类型为对应的标记
    def convertTag(self, compareName):
        tag, tagName, predictChange = convert.result(compareName, self.name, self.rateTitle)
        del self.rateTitle
        self.tag = [tag]
        #self.tagName = tagName ###之后可以删
        u""" 一些情况下可能需要更新概率 """
        if predictChange != None:
            self.predict_rate = predictChange

    def __repr__(self):
        return repr((self.__dict__))
