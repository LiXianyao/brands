#-*-coding:utf8-*-#
####近似名字对象
class goodsRegisterRate:
    def __init__(self, id, name, rate=100, rateName=None):
        self.id = id
        self.name = name
        self.rate = rate
        if rateName!=None:
            self.rateName = rateName

    ###更新较小值
    def updateRate(self, rate, rateName):
        if self.rate < rate:
            self.rate = rate
            self.rateName = rateName

    def __repr__(self):
        return repr((self.__dict__))
