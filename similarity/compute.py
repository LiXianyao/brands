#-*-coding:utf8-*-#
import sys
import brand
sys.path.append("..")
reload(sys)
sys.setdefaultencoding("utf-8")

similar_gate_low = 0.67
similar_gate_high = 0.8
py_combi_lowb = 0.5
py_rate_lowb = 0.3

# 中文编辑距离(越大越近)， 拼音编辑距离（越大越近）0.9， 包含被包含（越大越近）
# 排列组合（越大越近）， 中文含义近似（越大越近）0.9， 中文字形近似（越大越近）0.9
# 英文编辑距离(越大越近)， 英文包含被包含（越大越近）， 英文排列组合（越大越近）
# 数字完全匹配（越大越近）
u""" 默认门限值 """
default_gate = ['C', 0.8, 'C', 'C', 0.9, 0.9, 'C', 'C', 'C', 1.0]

# 计算两个输入商标的相似度
def compute_similar(brand_name, his_name, gate=default_gate):
    compare_Res = brand.getCharacteristics(brand_name, his_name)
    similar = False
    for index in range(len(compare_Res)):
        if gate[index] == 'C':
            if len(brand_name) < 4 and compare_Res[index] >= similar_gate_low:
                similar = True
            elif len(brand_name) >= 4 and compare_Res[index] >= similar_gate_high:
                similar = True
        elif gate[index] == 'N':
            continue
        else:
            if compare_Res[index] >= gate[index]:
                similar = True
    return similar,  compare_Res

u""" 扫描门限值，并返回第一个满足条件的门限值下标 """
def search_gate(attri_list, gate_list):
    for index in range(len(attri_list)):
        rate, title = attri_list[index]
        gate, title = gate_list[index]
        if gate == 'C':
            if rate >= similar_gate_low:
                return rate, title
        elif gate == 'N':
            continue
        else:
            if rate >= gate:
                return rate, title
    return None, None

###判断两个商标中是否有同音字
def judge_pinyin(brand_name_pinyin, his_name_pinyin):
    b_list = brand_name_pinyin
    h_list = his_name_pinyin.split(",")
    b_len = len(b_list)
    h_len = len(h_list)

    cnt_comm = 0
    if b_len <= 3:  ##商标长度小于等于3时，按乱序查找。即只要h串里有就行（可能重音，要标记）
        h_vis = [False] * (h_len)
        for i in range(b_len):
            for j in range(h_len):
                # print h_list[j], b_list[i], h_list[j] == b_list[i]
                if h_vis[j] == False and h_list[j] == b_list[i]:
                    cnt_comm += 1
                    h_vis[j] = True
                    break
    if b_len > 3:  ##商标长度大于等于3时，按正序查找（就是算最长匹配距离）
        cnt_comm = brand.maxMatchLen(b_list, h_list)

    print "py check ===> ", b_list, h_list, cnt_comm, b_len, h_len
    if h_len > cnt_comm + 2:  ##字数比较，被比较商标与输入商标，在公有部分的基础上长4以上就pass
        return False

    if b_len < 3 and cnt_comm > 0 and h_len < cnt_comm + 2:
        # 输入商标的长度只有1或者2， 那么共有部分必须是1或者2
        return True
    elif b_len >= 3 and cnt_comm >= max(int(len(b_list) * py_rate_lowb), 2):  #
        # 输入商标长度为3或者以上，那么部分重合就可以
        return True
    return False

###计算拼音共同下界
def compute_py_lowb(brand_name_pinyin):
    b_list = brand_name_pinyin

    if len(b_list) < 3:
        # print b_list, h_list, cnt_comm
        return max(len(b_list) - 1, 1)
    else:
        # print b_list,h_list
        return max(int(len(b_list) * py_combi_lowb), 2)