# coding=utf8

import re
from pypinyin import lazy_pinyin
from strFunction import get_china_str

def maxMatch(_str1, _str2):
    lstr1 = len(_str1)
    lstr2 = len(_str2)
    record = [[0 for i in range(lstr2 + 1)] for j in range(lstr1 + 1)]  # 多一位
    ###维度为 lstr1+1 * lstr2＋１　的矩阵。字符要转成ｕｎｉｃｏｄｅ
    maxNum = 0  # 最长匹配长度
    s1_p = 0  # 记录最大匹配长度在串1里的终止位置\
    s2_p = 0  # 记录最大匹配长度在串2里的终止位置

    for i in range(lstr1):
        for j in range(lstr2):
            if _str1[i] == _str2[j]:
                # 相同则累加
                record[i + 1][j + 1] = record[i][j] + 1
                if record[i + 1][j + 1] > maxNum:
                    # 获取最大匹配长度
                    maxNum = record[i + 1][j + 1]
                    # 记录最大匹配长度的终止位置
                    s1_p = i + 1
                    s2_p = j + 1
    return lstr1, lstr2, s1_p, s2_p, maxNum

def pinyin2(_str1,_str2):
    zhPattern = re.compile(u'[\u4e00-\u9fa5]+')

    # print zhPattern.search(_str1)
    # print zhPattern.search(_str2)

    if zhPattern.search(_str1):
        _str1 = ' '.join(lazy_pinyin(_str1))
    if zhPattern.search(_str2):
        _str2 = ' '.join(lazy_pinyin(_str2))

    if _str1 == _str2:
        return 8, u"声音相同字不同 8"     #声音相同字不同
    _str1 = _str1.split(" ")
    _str2 = _str2.split(" ")

    lstr1, lstr2, s1_p, s2_p, maxNum = maxMatch(_str1, _str2)
    ##输入名字被完全包含的情况：
    if lstr1 == maxNum:  #好中华 中华 名字前加字
        if s2_p == lstr2:
            return 9, u"声音相同字不同前面加字 9"   #名字前加字

        if s2_p == s1_p:   #中华好 中华 名字后加字
            return 10, u"声音相同字不同后面加字 10"  # 名字后加字
        ##剩余情况为：输入名字是近似商标的一部分
        return 12, u"名字的一部分声音相同字不同 12" #名字的一部分
    else: ##输入商标只有部分适配
        if s2_p == lstr2 and lstr2 > maxNum:   #名字的一部分前面加文字：商标2的后缀是1的一部分
            return 13, u"名字的一部分声音相同字不同，前面加文字 13" #名字的一部分前面加文字
        if s2_p == maxNum and lstr2 > maxNum:  # 商标2的前缀是1的一部分
            return 14, u"名字的一部分声音相同字不同，后面加文字 14"#名字的一部分后面加文字
        return 15, u"名字的一部分声音相同字不同，被包含 15"  # 名字中间加字

    """
    for index in range(len(_str1)):
        if index == 0 and _str1[index] == _str2:
            return 10, u"声音相同字不同后面加字 10"   #声音相同字不同后面加字
    return 9, u"声音相同字不同前面加字 9"    #声音相同字不同前面加字
    """

def contain(_str1,_str2):
    lstr1, lstr2, s1_p, s2_p, maxNum = maxMatch(_str1, _str2)

    ##输入名字被完全包含的情况：
    if lstr1 == maxNum:  #好中华 中华 名字前加字
        if s2_p == lstr2:
            return 2, u"名字前加字 2"   #名字前加字

        if s2_p == s1_p:   #中华好 中华 名字后加字
            return 3, u"名字后加字 3"  # 名字后加字
        ##剩余情况为：输入名字是近似商标的一部分
        return 4, u"名字被包含 4" #名字的一部分
    else: ##输入商标只有部分适配
        if s2_p == lstr2 and lstr2 > maxNum:   #名字的一部分前面加文字：商标2的后缀是1的一部分
            return 5, u"名字的一部分前面加文字 5" #名字的一部分前面加文字
        if s2_p == maxNum and lstr2 > maxNum:  # 商标2的前缀是1的一部分
            return 6, u"名字的一部分后面加文字 6"#名字的一部分后面加文字
        return 7, u"名字的一部分被包含 7"  # 名字中间加字


def result(_str1,_str2,lable):
    _str1 = get_china_str(_str1)
    _str2 = get_china_str(_str2)
    if _str1 == _str2:
        return 1, u"名字完全相同 1"
    elif lable == "汉字字形相似度":
        return 11, u"字形相近 11"
    elif lable == "拼音相似度":
        return pinyin2(_str1,_str2)
    elif lable == "汉字包含被包含":
        return contain(_str1,_str2)
    else:
        return 12, lable + u" 12"

if __name__ == "__main__":
    print "%s"% result(u"中华", u"中华好", "拼音相似度")[1]
    print "%s"% result(u"中华", u"好中华", "拼音相似度")[1]
    print "%s"% result(u"中华", u"大中华好", "拼音相似度")[1]
    print "%s"% result(u"他中华", u"大好他中", "拼音相似度")[1]
    print "%s"% result(u"中华人", u"中华家", "拼音相似度")[1]
    print "%s"% result(u"他是中华人", u"大中华", "拼音相似度")[1]