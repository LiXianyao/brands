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


# 判断是否为汉字字符串
# 存在汉字，判断为汉字字符串
from pypinyin import lazy_pinyin
import jieba
def isChina(_str):
    for ch in _str.decode("utf-8"):
        if u'\u4e00' <= ch <= u'\u9fff':
            return True
    return False

# 判断是否为数字字符串
# 必须为纯数字，字母夹数字不是数字字符串
def isNum(_str):
    return _str.isdigit()


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

#获得中文字符串
def get_china_str(_str):
    str = jieba.cut(_str, cut_all=False)
    str_china = ""
    for i in str:
        if isChina(i):
            str_china = str_china + i

    return str_china

#拆分非中文字符串，将相邻的字母视作一个词，相邻的数字视作一个数
def split_not_china(_str):
    split_list = re.findall(u"[a-zA-Z][a-z]+|[A-Z][A-Z]*|[0-9]+", _str)
    num_str = ""
    eng_str = ""
    eng_str_2 = ""
    str_l = len(_str)
    for str_i in split_list:
        if isNum(str_i):
            num_str += str_i
        else:
            str_i = str_i.lower()
            eng_str = eng_str + str_i + (" " * len(str_i))
            eng_str_2 = eng_str_2 + str_i + " "

    return num_str, eng_str, eng_str_2

#获得商标名中的英文（列表形式）
def get_not_china_list(_str):
    split_list = re.findall(u"[a-zA-Z][a-z]+|[A-Z][A-Z]*|[0-9]+", _str)
    num_list = []
    eng_list = []
    for str_i in split_list:
        if isNum(str_i):
            num_list.append(str_i)
        else:
            str_i = str_i.lower()
            eng_list.append(str_i)

    return num_list, eng_list



########################## 特征算法################################

#   中、英文编辑距离
from Levenshtein import *
def editDistance(_str1,_str2):
    if len(_str1) == 0 or len(_str2) == 0:
        return 0.0

    ##返回值是 1 - 编辑距离/较长串的长度
    return 1.0 - distance(_str1,_str2)/max(len(_str1),len(_str2))




# 中文包含或被包含关系
def inclusion(_str1,_str2):
    if len(_str1) == 0 or len(_str2) == 0:
        return 0

    maxNum = maxMatchLen(_str1, _str2)

    return maxNum/min(len(_str1),len(_str2))

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
def inclusion_Eng(_str1,_str2):
    if len(_str1) == 0 or len(_str2) == 0:
        return 0

    list1 = [x for x in _str1.split(" ")]
    list2 = [x for x in _str2.split(" ")]
    list1.pop()
    list2.pop()

    lstr1 = len(list1)
    lstr2 = len(list2)
    maxNum = maxMatchLen(list1, list2)

    return maxNum / min(lstr1,lstr2)

# 中 文字排列组合方式
def combination(_str1,_str2):
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
    return ans/min(len(list1),len(list2))

# 英文 文字排列组合方式
def combination_Eng(_str1,_str2):
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
    return ans/min(len(list1),len(list2))


# 中文拼音编辑距离
def pinyinEditDistance(_str1,_str2):
    if len(_str1) == 0 or len(_str2) == 0:
        return 0.0

    zhPattern = re.compile(u'[\u4e00-\u9fa5]+')
    if zhPattern.search(_str1):
        _str1 = ' '.join(lazy_pinyin(_str1))
    if zhPattern.search(_str2):
        _str2 = ' '.join(lazy_pinyin(_str2))
    #整个串内的中文转拼音后，求两个拼音（和原非中文）串/串1长度
    return 1.0 - distance(_str1,_str2)/max(len(_str1),len(_str2))


# 中文含义近似
# 返回值：[0-1]，并且越接近于1代表两个句子越相似
import synonyms
def implicationApproximation(_str1,_str2):
    if len(_str1) == 0 or len(_str2) == 0:
        return 0
    return synonyms.compare(_str1,_str2,seg=False) ##有没有必要分词呢？


# 中文字形相似
# 距离在[0，1]之间，0表示两个字笔画完全相同，1表示完全不同
import stroke
def glyphApproximation(_str1,_str2):
    if len(_str1) == 0 or len(_str2) == 0 or len(_str1) != len(_str2):
        return 0.00
    return stroke.get_dist(_str1,_str2)


# 数字完全匹配
def numTotalEqual(_str1,_str2):
    if len(_str1) == 0 or len(_str2) == 0:
        return 0.00
    if _str1 == _str2:
        return 1.00
    else:
        return 0.00

def getCharacteristics(_str1,_str2):
    str1_num, str1_char, str1_china,str1_char_2 =init_String(_str1)
    str2_num, str2_char, str2_china,str2_char_2 =init_String(_str2)


    #中文编辑距离(越大越近)， 拼音编辑距离（越大越近）， 包含被包含（越大越近）
    #排列组合（越大越近）， 中文含义近似（越大越近）， 中文字形近似（越大越近）
    #英文编辑距离(越大越近)， 英文包含被包含（越大越近）， 英文排列组合（越大越近）
    #数字完全匹配（越大越近）
    return round(editDistance(str1_china,str2_china),2),round(pinyinEditDistance(str1_china,str2_china),2),round(inclusion(str1_china,str2_china),2),\
           round(combination(str1_china,str2_china),2),round(implicationApproximation(str1_china,str2_china),2),round(glyphApproximation(str1_china,str2_china),2),\
           round(editDistance(str1_char,str2_char),2),round(inclusion_Eng(str1_char_2,str2_char_2),2),round(combination_Eng(str1_char_2,str2_char_2),2),round(numTotalEqual(str1_num,str2_num),2)


