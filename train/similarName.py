#-*-coding:utf8-*-#
from similarity import convert
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
    def __init__(self, compareName, name, attriList):
        self.name = name
        self.calMaxAttri(attriList)
        self.convertLabel(compareName)

    ###确定优先级最高的特征类型
    def calMaxAttri(self, attriList):
        attriList = zip(attriList, attribute_title)
        attriList.sort(key = sortNames)
        #print self.name
        #for  (rate, title) in attriList:
        #    print title , rate
        (self.maxAttri, self.maxAttriTitle) = (None, None)
        for (rate, title) in attriList:
            if rate >= 0.66:  ###先按照优先级顺序找第一个满足阈值的类别
                (self.maxAttri, self.maxAttriTitle) = (rate, title)
                break

        if (self.maxAttri, self.maxAttriTitle) == (None, None):
            (self.maxAttri, self.maxAttriTitle) = max(attriList, key=lambda x: x[0])

    ##转化特征类型为对应的标记
    def convertLabel(self, compareName):
        labelCode, label = convert.result(compareName, self.name, self.maxAttriTitle)
        self.labelCode = labelCode
        self.label = label

    def __repr__(self):
        return repr((self.name, self.labelCode, self.label, self.maxAttri))
