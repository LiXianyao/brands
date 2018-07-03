# -*- coding: utf-8 -*-
from sqlalchemy import Column,String, Integer, ForeignKey, Date, Text
from database import Base,db_session
class BrandHistory(Base):

    #表名
    __tablename__ ='brand_history'
    #表结构
    id = Column(Integer, primary_key=True, autoincrement=True)
    brand_name = Column(String(255))
    product_list = Column(Text)
    apply_date = Column(Date)
    i18n_type = Column(Integer, nullable=False)
    brand_status =  Column(Integer,nullable=True)
    csv_name = Column(Integer, nullable=True)
    # 查询构造器、、、
    query = db_session.query_property()

    def __init__(self, brand_name, product_list, apply_date, i18n_type, brand_status, csv_name):
        self.apply_date = apply_date
        self.product_list = product_list
        self.brand_name = brand_name
        self.i18n_type = i18n_type
        self.brand_status = brand_status
        self.csv_name = csv_name

    def __repr__(self):
        brand_dict = {
            u"brand_name": self.brand_name,
            u"product_list": self.product_list,
            u"apply_date":str(self.apply_date),
            u"i18n_type":self.i18n_type,
            u"brand_status":self.brand_status
        }
        return str(brand_dict)

    def __dir__(self):
        brand_dict = {
            u"brand_name": self.brand_name,
            u"product_list": self.product_list,
            u"apply_date": str(self.apply_date),
            u"i18n_type": self.i18n_type,
            u"brand_status": self.brand_status
        }
        return str(brand_dict)

