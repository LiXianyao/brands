# -*- coding: utf-8 -*-
from sqlalchemy import Column,String, Integer, ForeignKey, Date, Text
from database import Base, db_session
class BrandHistory(Base):

    #表名
    __tablename__ ='brand_history'
    #表结构
    id = Column(Integer, primary_key=True, autoincrement=True)
    brand_no = Column(String(10))
    brand_name = Column(String(128))
    apply_date = Column(String(8))
    class_no = Column(Integer, nullable=False)
    brand_status = Column(Integer, nullable=True)
    insert_status = Column(Integer, nullable=True)#1是成功插入，2是图形或空跳过，3是名字异常跳过
    # 查询构造器、、、
    query = db_session.query_property()

    def __init__(self, brand_no, brand_name, apply_date, class_no, brand_status, insert_status):
        self.brand_no = brand_no
        self.apply_date = apply_date
        self.class_no = class_no
        self.brand_name = brand_name
        self.brand_status = brand_status
        self.insert_status = insert_status

    def __repr__(self):
        brand_dict = {
            u"brand_name": self.brand_name,
            u"brand_no": self.brand_no,
            u"apply_date": str(self.apply_date),
            u"class_no": self.class_no,
            u"brand_status": self.brand_status,
            u"insert_status": self.insert_status
        }
        return str(brand_dict)

    def __dir__(self):
        brand_dict = {
            u"brand_name": self.brand_name,
            u"brand_no": self.brand_no,
            u"apply_date": str(self.apply_date),
            u"class_no": self.class_no,
            u"brand_status": self.brand_status,
            u"insert_status": self.insert_status
        }
        return str(brand_dict)

if __name__=="__main__":

    add_list = []
    """
    new_record = BrandHistory('34738019', u"图形", "20181116", 13, 0, 1)
    add_list.append(new_record)
    new_record = BrandHistory('34738019', u"图形", "20181116", 13, 0, 1)
    add_list.append(new_record)
    new_record = BrandHistory('34738019', u"图形", "20181116", 13, 0, 1)
    add_list.append(new_record)
    new_record = BrandHistory('34738019', u"图形", "20181116", 13, 0, 1)
    add_list.append(new_record)
    db_session.add_all(add_list)
    db_session.commit()"""
    brand_no = "1"
    update_record = db_session.query(BrandHistory).filter(BrandHistory.brand_no == brand_no).first()
    update_record.brand_status = 1
    add_list.append(update_record)
    db_session.add_all(add_list)
    db_session.commit()
