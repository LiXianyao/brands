#-*-coding:utf8-*-#
from database import db_session
from brand_record import BrandRecord
from brand_item import BrandItem
import csv
import time
import json
import  traceback

def load_brand_item():
    item_list = BrandItem.query.all()
    item_dict = {}
    for item in item_list:
        group_no = int(item.group_no)
        item_name = item.item_name
        item_no = item.item_no
        if group_no not in item_dict:
            item_dict[group_no] = {}
        item_dict[group_no][item_name] = item_no

    #for item in item_dict[101]:
    #    print item, item_dict[101][item]
    #print item_dict.keys()
    return item_dict

def form_brand_record():
    row_len = 9
    file_names = range(10, 11)
    batch = 500

    item_dict = load_brand_item()

    date_origi_format = "%Y年%m月%d日"
    date_target_format = "%Y-%m-%d"
    ##3,
    old = 0

    insertsql = "insert ignore into brand_record(apply_date, product, brand_name, i18n_type, brand_status) values" \
                " (:apply_date, :product, :brand_name, :i18n_type, :brand_status)"
    session = db_session()
    for file_name in file_names:
        csv_name = file_name
        file_name = str(file_name) + ".csv"
        with open("/root/csv3/" + file_name, "rU") as csv_file:
            reader = csv.reader(csv_file, delimiter=',', quotechar='\n')
            line_cnt = 0
            ok_cnt = 0
            execute_list = []
            for line in reader:
                line_cnt += 1
                if line_cnt == 1:
                    continue
                if len(line) < row_len:
                    print "line %d error, as:"%(line_cnt)
                    print line
                    continue

                try:
                    apply_date = time.strptime(line[-3], date_origi_format)
                    apply_date = time.strftime(date_target_format, apply_date)
                    i18n_type = line[-2]
                    brand_status = line[-1]
                    if len(brand_status) > 1:
                        continue

                    product_list_head = len(line) -5
                    product_list = line[product_list_head].replace("]\"","]").replace("\"[","[").replace("\"\"","\"")
                    flag = True
                    product_list_array = [{}]
                    while flag and product_list_head > 3:
                        try:
                            product_list_array = json.loads(product_list)
                            flag = False
                        except:
                            product_list_head -= 1
                            product_list = line[product_list_head].replace("]\"","]").replace("\"[","[").replace("\"\"","\"") \
                                           + "," + product_list
                    if flag == True:
                        continue
                    brand_name = ','.join(line[3: product_list_head])
                    if brand_name == "图形":
                        continue
                    for index in range(len(product_list_array)):
                        product_name = product_list_array[index]['product_name'].replace(u'（',u'(').replace(u'）',u')')
                        product_group = int(product_list_array[index]['product_group'])
                        try:
                            product_no = item_dict[product_group][product_name]
                            execute_list.append({"apply_date": apply_date,
                                                 "product": product_no,
                                                 "brand_name": brand_name,
                                                 "i18n_type": i18n_type,
                                                 "brand_status": brand_status})
                            ok_cnt += 1
                        except:
                            pass
                            #print product_group, product_name
                            #print apply_date
                except Exception,e:
                    print traceback.format_exc()

                if line_cnt % batch == 0 and line_cnt >= old:
                    try:
                        session.execute(insertsql, execute_list)
                        del execute_list[:]
                        execute_list = []
                        db_session.commit()
                    except:
                        print "csv_file " + file_name + " produce %d rows" % (line_cnt)
                        print "error!!!", traceback.format_exc()
                #break

            try:
                session.execute(insertsql, execute_list)
                del execute_list[:]
                execute_list = []
                db_session.commit()
            except:
                print "error!!!", traceback.format_exc()
            print "csv_file " + file_name + " has %d rows, legal rows are %d"%(line_cnt, ok_cnt)


##975418个不同的商标，12277622
if __name__=="__main__":
    form_brand_record()
    #load_brand_item()






