# coding=utf8
"""
This module provide two functions:
    1. Get stroke string for chinese character strings,
    2. Get distance between two different chinese character strings.
Authors: IVoid Song(songxin02@baidu.com)
DATE:    2015-03-31
"""
__all__ = ['get', 'get_dist']

import os
import sys
import logging
import numpy as np

# 初始化汉字笔画字典stroke_code_book
stroke_code_book = dict()
code_book = os.path.join(os.path.dirname(__file__), "stroke.code_book")
try:
    with open(code_book, 'rb') as f:
        for line in f:
            vec = line.strip().split('\t')
            if len(vec) != 2:
                #logging.error("笔画码表格式错误，文件:" + code_book + ",记录:" + line)
                sys.exit(1)
            stroke_code_book[vec[1].decode('utf8')] = vec[0]
except IOError as e:
    #logging.error("Can't found " + code_book)
    sys.exit(1)


def isChinese(s, encoding='utf8'):
    try:
        chars = s.decode(encoding)
    except UnicodeDecodeError as e:
        return False

    for char in chars:
        if char != ' ' and char not in stroke_code_book:
            return False
    return True


def get(s, delimiter="", encoding='utf8'):
    """返回汉字字符串s对应的笔画字符串.
    Args:
        s: 汉字字符串
        delimiter: 返回的笔画字符串，不同汉字之间的间隔
        encoding: 汉字字符串s的编码方式
    Return:
        字符串，汉字字符串s对应的笔画字符串
    Attention:
        注意，如果s中包含不为汉字的字符，则该对应字符的笔画会返回空字符串。
    """
    # return delimiter.join(_stroke_generator(s, encoding))
    return _stroke_generator(s, encoding)


def get_dist(charsA, charsB):
    """计算两个汉字字符串charsA和charsB之间笔画的编辑距离。
    Args:
        charsA：汉字字符串A
        charsB: 汉字字符串B
    Return:
        位于[0,1]之间的浮点数，表示charsA和charsB之间笔画的编辑距离。0表示完全一致，1表示完全不同。
    Attention:
        注意，若charsA和charsB之间只差一个偏旁部首，则其编辑距离减半。

    """
    seqA = get(charsA)
    seqB = get(charsB)
    for i in seqA:
        if "" in seqA:
            seqA.remove("")

    for i in seqB:
        if "" in seqB:
            seqB.remove("")

    if len(seqA) ==0 or len(seqB) == 0:
        return 0.0

    result = []
    vis_B = []
    for i in range(len(seqB)):
        vis_B.append(False)

    for i in range(len(seqA)):
        s1 = seqA[i]
        answer = []
        for j in range(len(seqB)):
            if vis_B[j] == True:
                answer.append(0.0)
                continue
            s2 = seqB[j]
            #print s1, s2, 1.0 - _stroke_edit_dist(s1, s2) * 1.0/max(len(s1),len(s2))
            try:
                answer.append(1.0 - _stroke_edit_dist(s1, s2) * 1.0/max(len(s1),len(s2)))
            except:  ###不知道为什么，避不开上面会在一些时候抛除以 0 的错误
                answer.append(0.0)
        if len(answer) > 0:
            result.append(max(answer))
            vis_B[answer.index(max(answer))] = True

    if len(result) == 0:
        result = [0.0]
    return np.mean(result)


def _stroke_generator(chars, encoding):
    """生成汉字字符串chars的笔画序列.如果chars中包含非中文字符，则对应字符的笔画会被设置为空字符串。
    Args:
        chars: 字符串
        encoding：chars的编码方式
    Return:
        迭代器，生成字符串chars的笔画
    """
    try:
        chars = chars.decode(encoding)
    except UnicodeDecodeError as e:
        #logging.error("Incorrect encodings: " + chars + "  " + encoding)
        raise StopIteration

    list = []

    for char in chars:
        if char in stroke_code_book:
            list.append(stroke_code_book[char])
        else:
            list.append("")
    return list


def _stroke_edit_dist(seqA, seqB):
    """利用动态规划来计算seqA和seqB的编辑距离。
    Args:
        seqA: 笔画序列A
        seqB：笔画序列B
    Return：
        整数，笔画序列A和笔画序列B的编辑距离
    """
    if seqA == "":
        return len(seqB)
    if seqB == "":
        return len(seqA)

    pre = range(0, len(seqA) + 1)
    post = [0] * (len(seqA) + 1)
    for i in range(1, len(seqB) + 1):
        post[0] = i
        for j in range(1, len(seqA) + 1):
            if seqB[i - 1] == seqA[j - 1]:
                post[j] = pre[j - 1]
            else:
                post[j] = min(post[j - 1] + 1, pre[j - 1] + 1, pre[j] + 1)
        pre = post[:]
    return post[len(seqA)]
