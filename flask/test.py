# -*- coding:utf-8 -*-#
import requests
import json
import threading
import time

import os
'''
payload = {'itemId': '1548973246'}#0071809252
r = requests.get("http://0.0.0.0:5000/spider", params=payload)
print r.text
'''
'''
form = {'text': '{"detail":['
                   '{"date":"2017.09.11","format":"kindle","review":'
                   '    "Dresses rarely fit me well because of a large bust. I\'m normally a 14 and the large fits me like it was made for me. It is very pretty and summery in color. The skirt half hits my mid knee which is perfect for work. This is a no wrinkle, cheerful dress with just the right sleeve. Th Princess seams give this knit shape and form. I really love it, it is a new favorite.I want to add that this is quite heavy with essentially 2 layers of knit forming the fabric. For me in Wisconsin this is a 3 season dress. However in a warmer state this might feel hot in the summer. I was surprised by the heft of the dress although it works for me. Very pleased.I want to add that this is quite heavy with essentially 2 layers of knit forming the fabric. For me in Wisconsin this is a 3 season dress. However in a warmer state this might feel hot in the summer. I was surprised by the heft of the dress although it works for me. Very pleased.My wife really likes this dress. It fits well and the garment is well made. As it is a simple design, it is a very versatile garment and she has taken it to wear on formal nights on a cruise, and to dinner matched with pearls. It travels well with no wrinkles and the advantage is it is washable.My wife really likes this dress. It fits well and the garment is well made. As it is a simple design, it is a very versatile garment and she has taken it to wear on formal nights on a cruise, and to dinner matched with pearls. It travels well with no wrinkles and the advantage is it is washable."},'
                   '{"date":"2017.09.12","format":"paper","review":"yy"}]}'}
'''
payload = {"text":
            '{"msg_list":'
                '['
                    '{"content": "恭喜您，您的双色球订单889775077560700928已中奖，奖金金额为5元，彩票站将会在24小时内为您兑奖并打款到您的奖金账户中，请注意查询您绑定的支付宝或者银行卡账户。如有问题请与彩票站联系13121029585或关注官方微信服务号（58彩讯）24小时客服为您服务。【58同城】"},'
                    '{"content": "【晓事科技】亲，感谢您使用晓事！您的注册验证码为7483。晓事，让县城生活更美好！退订请回复TD"},'
                    '{"content": "【Owhat应用】【这个是啥】亲爱的会员测试导入5为了您能优先参加应援活动和购买会员限定商品请使用该手机号尽快注册Owhat账号注册之后即可属于您的会员福利O~下载地址："},'
                    '{"content": "【莱迪机器人】荣乐ERP管理系统温馨提示：【郭恒】向您提交销售出货单：【XS-201700079】，需要您尽快审核!"},'
                    '{"content": "【e签证】尊敬的客户：感谢您选择e签证办理美国十年多次往返签证，办理流程已经发至您的邮箱；请您登录http://www.evisa99.com办理，用户名为本机号码，密码：341e34。客服电话：4000-518-111"},'
                    '{"content": "【滴滴出行】十元滴滴快车新手券已到账，7天有效，首次使用快车约3公里免费！快车更经济，更便捷，司机严挑选，行程可分享，也可开发票 退订TD"},'
                    '{"content": "【阿里云】云栖大会第二日，AI与IOT重磅发布，创业峰会、生态与金融峰会展示顶级生态。点击观看：http://tb.cn/RZtXr1x 回td退订"},'
                    '{"content": "【亿宝贷】您的验证码是627965"},'
                    '{"content": "【网易】绮美时光，有你相伴。阴阳师周年庆9月27日重磅开启！SSR式神玉藻前登场！SSR召唤概率3倍UP、未收录SSR式神降临、SSR重登神龛！登录签到，赢取庭院皮肤及SSR式神！详情 http://yys.163.com/m/mail 。回复TD退订"},'
                    '{"content": "【中信信用卡】尊敬的客户，中信银行特邀您办理中信易卡（金卡），点击链接 http://t.cn/ROVOaAO 立刻办理，回TD退订"}'
                ']'
            '}'
           }

check_name = {"text":
            '{"msg_list":'
                '['
                    '{"content": "嗨，我是南京强纳食品贸易有限公司HR，邀您投递办公室文员（5k）  https://i.58.com/1KxFrR  退订回11 【中华英才网】", "mobile":"18976753003"},'
                    '{"content": "【中银三星人寿】尊敬的刘晓乐女士：您2017年07月31日的投保申请10001*****（险种/保额：中银三星祥泰意外骨折医疗保险/1万元，合计保险费:99.0元，交费方式：年交;保险期间：1年）已于08月01日收费成功，合同生效日为2017年08月02日，电子版保险合同已发送至您指定的邮箱（459794806@qq.com），同时您可登陆公司网站查询保单信息并下载电子合同(下载路径：客户服务—在线业务办理—电子保单下载)，请您认真阅读保险条款，如有疑问请致电客服电话95566-7。搜索并关注公司官方微信“中银三星人寿”，可享受保单查询、在线保单变更、在线理赔申请等更多不一样的E服务。"},'
                    '{"content": "【方正宽带】【方正宽带】173129-故障#T - 铁东E2小区#5-1-2右#380042482#李树平#有设备-网络不畅用户不配合操作#13654495652"},'
                    '{"content": "【佰仟租赁】赵方凯先生：您已严重逾期，我司通过多种方式均无法与您取得联系，请速联系我司协商还款事宜，否则我司有理由相信您故意拖欠融资租赁款项，我司将立即解除汽车融资租赁合同，同时您将承担由于不良信用记录引起的不良后果。详询4009987103【佰仟租赁】"},'
                    '{"content": "【莱迪机器人】荣乐ERP管理系统温馨提示：【郭恒】向您提交销售出货单：【XS-201700079】，需要您尽快审核!"},'
                    '{"content": "【常州农委】翟云忠：翟云忠的市农委公务出差审批单请您及时办理。"},'
                    '{"content": "【台州水司】尊敬的客户:您好贵户(客户号:108412地址:富强村中街125#（客户号6142）)本期用水量7吨水费26.74元.(可使用支付宝、微信、银行代扣等方式缴费)"},'
                    '{"content": "【阿里云】云栖大会第二日，AI与IOT重磅发布，创业峰会、生态与金融峰会展示顶级生态。点击观看：http://tb.cn/RZtXr1x 回td退订"},'
                    '{"content": "【北京现代】from 潘雪冬 : 领导好：（2）二工厂8/9日（周二）夜班生产现况（8/10日早6点）：当日生产线运营实际：冲压S1：8+8（516SPH）、冲压S2：8+8（500SPH）；车身：8+8（94％);涂装：8+8（98％）；总装：8+8（100％）"},'
                    '{"content": "【市社保卡中心】您或您家人的社保卡已发放到新华区赵一街村村民委员会，请尽快携带身份证件领取和启用。若已领卡，请忽略此信息。咨询电话：12333、89632715。"},'
                    '{"content": "【中信信用卡】尊敬的客户，中信银行特邀您办理中信易卡（金卡），点击链接 http://t.cn/ROVOaAO 立刻办理，回TD退订"}'
                ']'
            '}'
           }
change = {"text":
            {"file_list":
                    {"labelFile": "201710261216_business_label.txt",
                     "modelFile": "201710261216_round_750_max_depth_5_industry.model",
                     "dictFile" : "201710261216_diction_business.txt"},
              "taskType" : "1"
            }
           }


update = {"text":
            {"execute": "1"}
           }
test1 = {"text":
            {
                "msg_list": [{"content":"【国民银行】您好，您于2017-08-14 10:51:08成功开立尾号0823账户，并存入人民币1.00元。","id":"1","mobile":"17600106536"}]
             }
        }
test2 = {"text":
            {
                "msg_list": [{"content":"【常州农委】翟云忠：翟云忠的市农委公务出差审批单请您及时办理","id":"2","mobile":"17600106536"}]
             }
        }
test3 = {"text":
            {
                "templateMatching":True,
                "extraInformation":True,
                "msg_list": [{"content":"【莱迪机器人】荣乐ERP管理系统温馨提示：【郭恒】向您提交销售出货单：【XS-201700079】，需要您尽快审核!","id":"3","mobile":"17600106536"},
                             {"content": "【国民银行】您好，您于2017-08-14 10:51:08成功开立尾号0823账户，并存入人民币1.00元。", "id": "1",
                              "mobile": "17600106536"},
{"content":"【常州农委】翟云忠：翟云忠的市农委公务出差审批单请您及时办理","id":"2","mobile":"17600106536"},
{"content":"【佰仟租赁】赵方凯先生：您已严重逾期，我司通过多种方式均无法与您取得联系，请速联系我司协商还款事宜，否则我司有理由相信您故意拖欠融资租赁款项，我司将立即解除汽车融资租赁合同，同时您将承担由于不良信用记录引起的不良后果。详询4009987103【佰仟租赁】","id":"4","mobile":"17600106536"}
                             ]
             }
        }
test4 = {"text":
            {

                "msg_list": [{"content":"【莱迪机器人】荣乐ERP管理系统温馨提示：【郭恒】向您提交销售出货单：【XS-201700079】，需要您尽快审核!","id":"4","mobile":"17600106536"}]
             }
        }
test5 = {"text":
                {
                    "name": "柠檬轩",
                    "class": [43,44],
            "apply_date":"2018年01月05日"

            }
           }

test6 = {"text":
            {

                "msg_list":
                [
                    {"content": "叶林平,福建兰庭房产代理有限公司邀您投递【福州仓山区榕城广场-电话销售】月薪面议_面议!五险一金,包住,交通补助！找工作回复数字‘1’申请投递。职位详情：（https://i.58.com/2jSNNG）【回复TD退订】【58同城】"}
                    ]
            }
}
test7 = {"text":
            {

                "msg_list":
                [
                    {"content": "刘薇,石家庄霓霖贸易有限公司邀您投递【石家庄裕华区-淘宝客服】月薪3000_6000!五险一金,包吃,年底双薪,饭补,加班补助！找工作回复数字‘1’申请投递。职位详情：（https://i.58.com/2jI4rI）【回复TD退订】【58同城】"}
                    ]
            }
}
test8 = {"text":
            {

                "msg_list":
                [
                    {"content": "【中信银行】欢迎使用中信银行南昌分行提供的免费上网服务，您的验证码为：7791，验证码30分钟内有效，请妥善保管。"},
                    {"content": "【中信银行】尊敬的客户，本次动态口令为433826，请勿向他人泄露您的验证码"},
                    {"content": "【中信银行】海口分行 3-11 13:00:30,美兰机场自助网点[24.157.130.1],设备状态==DOWN,并且连续出现5次以上"},
                    {"content": "【中信银行】机房巡检"}
                    ]
            }
}


cnt  = 1
#39.106.135.227
#47.95.32.216
#127.0.0.1
#r = requests.post("http://10.109.246.100:5001/industryClassification", json=test5)
r = requests.post("http://10.109.246.100:5001/industryClassification", json=test5)
#r = requests.post("http://10.109.246.100:5002/templateMatching", json=test8)
return_msg = json.loads(r.text)
print str(return_msg).replace('u\'', '\'').decode("unicode-escape")
