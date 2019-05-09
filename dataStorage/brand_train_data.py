# -*- coding: utf-8 -*-
from sqlalchemy import Column,String, Integer, Boolean
from history_database  import Base,db_session
class BrandTrainData(Base):

    #表名
    __tablename__ ='brand_train_data'
    #表结构
    id = Column(Integer,autoincrement=True, primary_key=True)
    brand_no = Column(String(12))
    brand_name = Column(String(32))
    apply_date = Column(String(8))
    brand_sts = Column(Boolean)
    class_no = Column(Integer,nullable=True)
    his_no = Column(String(12))
    his_name = Column(String(32))
    his_date = Column(String(8))
    his_sts = Column(Boolean)
    similarity = Column(String(256))
    # 查询构造器
    query = db_session.query_property()

    def __init__(self, brand_no, brand_name, brand_sts, apply_date, class_no, his_no,
                 his_name, his_sts, his_date, similarity):
        self.brand_no = brand_no
        self.brand_name = brand_name
        self.brand_sts = brand_sts
        self.apply_date = apply_date
        self.class_no = class_no
        self.his_no = his_no
        self.his_name = his_name
        self.his_sts = his_sts
        self.his_date = his_date
        self.similarity = similarity

    def checkSegment(self):
        if len(self.brand_name) * 3 > 64:
            return False
        if len(self.his_name) * 3 > 64:
            return False
        return True

    def __repr__(self):
        brand_dict = {
            u"brand_no": self.brand_no,
            u"brand_name": self.brand_name,
            u"class_no": int(self.class_no)
        }
        return str(brand_dict)

    def __dir__(self):
        brand_dict = {
            u"brand_no": self.brand_no,
            u"brand_name": self.brand_name,
            u"class_no": int(self.class_no)
        }
        return str(brand_dict)

