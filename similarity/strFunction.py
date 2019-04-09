# coding=utf8
import re
import jieba


# 判断是否为汉字字符串
# 存在汉字，判断为汉字字符串
def isChina(_str):
    for ch in _str.decode("utf-8"):
        if u'\u4e00' <= ch <= u'\u9fff':
            return True
    return False


# 判断是否为数字字符串
# 必须为纯数字，字母夹数字不是数字字符串
def isNum(_str):
    return _str.isdigit()


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
    split_list = re.findall(u"[a-zA-Z][a-z]*|[A-Z][A-Z]*|[0-9]+", _str)
    num_list = []
    eng_list = []
    character_set = set()
    for str_i in split_list:
        if isNum(str_i):
            num_list.append(str_i)
        else:
            str_i = str_i.lower()
            eng_list.append(str_i)
            for letter in str_i:
                character_set.add(letter)
    return num_list, eng_list, character_set
