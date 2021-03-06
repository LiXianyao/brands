# coding=utf8
# 算法思路
# 1.使用空格、分号作为分隔符将字符串分开，拆分为汉字、英文、数字三个数组
# 2.特征分类 编辑距离 包含被包含 组合排序
#   汉字特有拼音编辑距离 含义近似 字形相似，其他默认0
#   数字特有完全匹配
#
# 、 包含算法默认为0 排列算法默认为 0
# f=open("log.txt","w")
from __future__ import division
import re
from pypinyin import lazy_pinyin, Style
import jieba
from strFunction import isChina, split_not_china
import time
import math
timeDefault = False


def init_String(_str):
    str = jieba.cut(_str, cut_all=False)
    str_num = ""
    str_char = ""
    str_china = ""
    str_char_2 = ""  ##英文单词用空格分开，用在英文排列组合中
    for i in str:
        #print i
        # 字符串拼接
        if isChina(i):
            str_china = str_china + i
        else:
            if i == u" " or i == u"":
                continue
            s_num_str, s_eng_str, s_eng_str_2 = split_not_china(i) ###非中文串，可能是单纯的数字或者字母串，也可能是混合串，拆一下
            str_char = str_char + s_eng_str
            str_char_2 = str_char_2 + s_eng_str_2
            str_num = str_num + s_num_str

    return str_num,str_char,str_china,str_char_2


########################## 特征算法################################

#   中、英文编辑距离
from Levenshtein import distance
def editDistance(_str1,_str2, ctime=timeDefault):
    start = time.time()
    if len(_str1) == 0 or len(_str2) == 0:
        return 0.0

    ##返回值是 1 - 编辑距离/较长串的长度
    res = 1.0 - distance(_str1, _str2)/max(len(_str1), len(_str2))
    if ctime:
        init_end = time.time()
        init_cost = init_end - start
        print("editDistance cost time: %.2fs" % (init_cost))
    return res




# 中文包含或被包含关系
def inclusion(_str1,_str2, ctime=timeDefault):
    start = time.time()
    if len(_str1) == 0 or len(_str2) == 0:
        return 0

    maxNum = maxMatchLen(_str1, _str2)
    res = maxNum/min(len(_str1),len(_str2))
    if ctime:
        init_end = time.time()
        init_cost = init_end - start
        print("inclusion ch cost time: %.2fs" % (init_cost))
    return  res

###计算最大匹配长度
def maxMatchLen(_str1, _str2):
    lstr1 = len(_str1)
    lstr2 = len(_str2)
    record = [[0 for i in range(lstr2 + 1)] for j in range(lstr1 + 1)]  # 多一位
    ###维度为 lstr1+1 * lstr2＋１　的矩阵。字符要转成ｕｎｉｃｏｄｅ
    maxNum = 0  # 最长匹配长度
    p = 0  # 匹配的起始位

    for i in range(lstr1):
        for j in range(lstr2):
            if _str1[i] == _str2[j]:
                # 相同则累加
                record[i + 1][j + 1] = record[i][j] + 1
                if record[i + 1][j + 1] > maxNum:
                    # 获取最大匹配长度
                    maxNum = record[i + 1][j + 1]
                    # 记录最大匹配长度的终止位置
                    p = i + 1
    return maxNum


# 英文 文字包含或被包含关系
def inclusion_Eng(_str1,_str2,ctime=timeDefault):
    start = time.time()
    if len(_str1) == 0 or len(_str2) == 0:
        return 0

    list1 = [x for x in _str1.split(" ")]
    list2 = [x for x in _str2.split(" ")]
    list1.pop()
    list2.pop()

    lstr1 = len(list1)
    lstr2 = len(list2)
    maxNum = maxMatchLen(list1, list2)
    res = maxNum / min(lstr1,lstr2)
    if ctime:
        init_end = time.time()
        init_cost = init_end - start
        print("inclusion_eng cost time: %.2fs" % (init_cost))
    return res

# 中 文字排列组合方式
def combination(_str1,_str2, ctime=timeDefault):
    start = time.time()
    if len(_str1) == 0 or len(_str2) == 0:
        return 0

    list1 = list(_str1)
    list2 = list(_str2)
    vis_list2 = [False] * len(list2)
    ans = 0
    for char_1 in list1:
        for i_2 in range(len(list2)):
            if (vis_list2[i_2] == True)or (char_1 != list2[i_2]):
                continue
            ans += 1
            vis_list2[i_2] = True
            break
    res = ans/min(len(list1),len(list2))
    if ctime:
        init_end = time.time()
        init_cost = init_end - start
        print("combination ch cost time: %.2fs" % (init_cost))
    return res

# 英文 文字排列组合方式
def combination_Eng(_str1,_str2, ctime=timeDefault):
    start = time.time()
    if len(_str1) == 0 or len(_str2) == 0:
        return 0

    list1 = [x for x in _str1.split(" ")]
    list2 = [x for x in _str2.split(" ")]

    list1.pop()
    list2.pop()
    vis_list2 = [False] * len(list2)
    ans = 0
    for char_1 in list1:
        for i_2 in range(len(list2)):
            if (vis_list2[i_2] == True)or (char_1 != list2[i_2]):
                continue
            ans += 1
            vis_list2[i_2] = True
            break
    res = ans/min(len(list1),len(list2))
    if ctime:
        init_end = time.time()
        init_cost = init_end - start
        print("combination eng cost time: %.2fs" % (init_cost))
    return res


# 中文拼音编辑距离
def pinyinEditDistance(_str1,_str2, ctime=timeDefault):
    start = time.time()
    if len(_str1) == 0 or len(_str2) == 0:
        return 0.0

    zhPattern = re.compile(u'[\u4e00-\u9fa5]+')
    if zhPattern.search(_str1):
        _str1 = ' '.join(lazy_pinyin(_str1, style=Style.TONE3))
    if zhPattern.search(_str2):
        _str2 = ' '.join(lazy_pinyin(_str2, style=Style.TONE3))
    #整个串内的中文转拼音后，求两个拼音（和原非中文）串/串1长度
    res = 1.0 - distance(_str1,_str2)/max(len(_str1),len(_str2))
    if ctime:
        init_end = time.time()
        init_cost = init_end - start
        print("py editDis cost time: %.2fs" % (init_cost))
    return res


# 中文含义近似
# 返回值：[0-1]，并且越接近于1代表两个句子越相似
import synonyms
def implicationApproximation(_str1,_str2, ctime=timeDefault):
    start = time.time()
    if len(_str1) == 0 or len(_str2) == 0:
        return 0
    """
    res = synonyms.compare(_str1, _str2, seg=False, ignore=True) ##有没有必要分词呢？
    if ctime:
        init_end = time.time()
        init_cost = init_end - start
        print("synonyms cost time: %.2fs" % (init_cost))
    if math.isnan(res):
        return 0.0
    """
    res = 0.0
    return res


# 中文字形相似
# 距离在[0，1]之间，0表示两个字笔画完全相同，1表示完全不同
import stroke
def glyphApproximation(_str1,_str2, ctime=timeDefault):
    start = time.time()
    if len(_str1) == 0 or len(_str2) == 0 or len(_str1) != len(_str2):
        return 0.00
    res = stroke.get_dist(_str1,_str2)
    if ctime:
        init_end = time.time()
        init_cost = init_end - start
        print("stroke cost time: %.2fs" % (init_cost))
    return res


# 数字完全匹配
def numTotalEqual(_str1,_str2):
    if len(_str1) == 0 or len(_str2) == 0:
        return 0.00
    if _str1 == _str2:
        return 1.00
    else:
        return 0.00

def getCharacteristics(_str1,_str2, ctime=timeDefault):
    start = time.time()
    str1_num, str1_char, str1_china,str1_char_2 =init_String(_str1)
    str2_num, str2_char, str2_china,str2_char_2 =init_String(_str2)


    #中文编辑距离(越大越近)， 拼音编辑距离（越大越近）， 包含被包含（越大越近）
    #排列组合（越大越近）， 中文含义近似（越大越近）， 中文字形近似（越大越近）
    #英文编辑距离(越大越近)， 英文包含被包含（越大越近）， 英文排列组合（越大越近）
    #数字完全匹配（越大越近）
    res = round(editDistance(str1_china,str2_china),2),round(pinyinEditDistance(str1_china,str2_china),2),round(inclusion(str1_china,str2_china),2),\
           round(combination(str1_china,str2_china),2),round(implicationApproximation(str1_china,str2_china),2),round(glyphApproximation(str1_china,str2_china),2),\
           round(editDistance(str1_char,str2_char),2),round(inclusion_Eng(str1_char_2,str2_char_2),2),round(combination_Eng(str1_char_2,str2_char_2),2),round(numTotalEqual(str1_num,str2_num),2)
    if ctime:
        init_end = time.time()
        init_cost = init_end - start
        print("totally cost time: %.2fs" % (init_cost))
    return res

if __name__=="__main__":
    brand_name = u"和尚志x"
    his_name = u"x志尚和"
    timeDefault = True
    print getCharacteristics(brand_name, his_name)  ###构造返回结果：近似商标名（及特征）
