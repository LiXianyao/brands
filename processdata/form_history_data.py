#-*-coding:utf8-*-#
from database import db_session
from brand_record import BrandRecord
from brand_history import BrandHistory
import csv
import time
import json


def form_brand_record():
    row = BrandHistory.query.count()
    print row

    batch = 10
    loopUpb = row / batch + 1
    loop = 0
    for i in range(loopUpb):
        offset = loop * batch
        limit = batch
        data = BrandHistory.query.offset(offset).limit(limit).all()
        for j in range(len(data)):
            print data[j]
        break

def form_brand_history():
    row_len = 9
    file_names = range(2, 14)
    batch = 10000

    date_origi_format = "%Y年%m月%d日"
    date_target_format = "%Y-%m-%d"
    ##3,
    old = 0

    for file_name in file_names:
        csv_name = file_name
        file_name = str(file_name) + ".csv"
        with open("/root/csv3/" + file_name, "rU") as csv_file:
            reader = csv.reader(csv_file, delimiter=',', quotechar='\n')
            line_cnt = 0
            ok_cnt = 0
            for line in reader:
                line_cnt += 1
                if line_cnt == 1:
                    continue
                if len(line) < row_len:
                    print "line %d error, as:"%(line_cnt)
                    print line
                    continue

                try:
                    brand_no = line[2]
                    brand_name = line[3]
                    related_group = line[-4]
                    apply_date = time.strptime(line[-3], date_origi_format)
                    apply_date = time.strftime(date_target_format, apply_date)
                    i18n_type = line[-2]
                    brand_status = line[-1]
                    if len(brand_status) > 1:
                        continue

                    product_list_head = len(line) -5
                    product_list = line[product_list_head].replace("]\"","]").replace("\"[","[").replace("\"\"","\"")
                    flag = True
                    while flag and product_list_head > 3:
                        try:
                            json.loads(product_list)
                            flag = False
                        except:
                            product_list_head -= 1
                            product_list = line[product_list_head].replace("]\"","]").replace("\"[","[").replace("\"\"","\"") \
                                           + "," + product_list
                    if flag == True:
                        continue
                    brand_name = ','.join(line[3: product_list_head])
                    #print brand_name
                    if brand_name == "图形":
                        continue

                    new_history = BrandHistory(brand_name, product_list,
                                               apply_date, i18n_type, brand_status, csv_name)
                    db_session.add(new_history)
                    ok_cnt += 1
                except:
                    print line_cnt, line

                if line_cnt % batch == 0 and line_cnt >= old:
                    print "csv_file " + file_name + " produce %d rows" % (line_cnt)
                    try:
                        db_session.commit()
                    except:
                        print "error!!!",line_cnt, line

            try:
                db_session.commit()
            except:
                print "error!!!",line_cnt, line
            print "csv_file " + file_name + " has %d rows, legal rows are %d"%(line_cnt, ok_cnt)

        #break

if __name__=="__main__":
    form_brand_history()






