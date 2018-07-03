# -*- coding: utf-8 -*-
from sqlalchemy import Column,String, Integer, ForeignKey, Date
from database import Base,db_session
class BrandRecord(Base):

    #表名
    __tablename__ ='brand_record'
    #表结构
    apply_date = Column(Date,primary_key=True)
    product = Column(String(16), primary_key=True)
    brand_name = Column(String(256),primary_key=True)
    i18n_type = Column(Integer, nullable=False)
    brand_status =  Column(Integer,nullable=True)
    # 查询构造器、、、
    query = db_session.query_property()

    def __repr__(self):
        brand_dict = {
            u"apply_date":str(self.apply_date),
            u"product":self.product,
            u"brand_name":self.brand_name,
            u"i18n_type":self.i18n_type,
            u"brand_status":self.brand_status
        }
        return str(brand_dict)

    def __dir__(self):
        brand_dict = {
            u"apply_date": str(self.apply_date),
            u"product": self.product,
            u"brand_name": self.brand_name,
            u"i18n_type": self.i18n_type,
            u"brand_status": self.brand_status
        }
        return str(brand_dict)
