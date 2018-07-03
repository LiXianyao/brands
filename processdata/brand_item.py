# -*- coding: utf-8 -*-
from sqlalchemy import Column,String, Integer, ForeignKey, Date
from history_database  import Base,db_session
class BrandItem(Base):

    #表名
    __tablename__ ='brand_item'
    #表结构
    id = Column(Integer,autoincrement=True, primary_key=True)
    item_no = Column(String(8))
    item_name = Column(String(64))
    group_no =  Column(Integer,nullable=True)
    class_no = Column(String(4))
    # 查询构造器、、、
    query = db_session.query_property()

    def __init__(self, item_no, item_name, group_no, class_no):
        self.item_no = item_no
        self.item_name = item_name
        self.group_no = group_no
        self.class_no = class_no

    def __repr__(self):
        brand_dict = {
            u"item_no": self.item_no,
            u"item_name": self.item_name,
            u"group_no": int(self.group_no),
            u"class_no": int(self.class_no)
        }
        return str(brand_dict)

    def __dir__(self):
        brand_dict = {
            u"item_no": self.item_no,
            u"item_name": self.item_name,
            u"group_no": int(self.group_no),
            u"class_no": int(self.class_no)
        }
        return str(brand_dict)

