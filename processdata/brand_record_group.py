# -*- coding: utf-8 -*-
from sqlalchemy import Column,String, Integer, ForeignKey, Date
from database import Base,db_session, db_engine
from sqlalchemy.ext.declarative import declarative_base
class BrandRecord(object):

    #表名
    __tablename__ ='brand_record'
    #表结构
    product = Column(String(16), primary_key=True)
    apply_date = Column(Date,primary_key=True)
    brand_name = Column(String(127),primary_key=True)
    i18n_type = Column(Integer, nullable=False)
    brand_status =  Column(Integer,nullable=True)
    group_table_dict = {}
    # 查询构造器、、、
    query = db_session.query_property()

    def __repr__(self):
        brand_dict = {
            u"apply_date":str(self.apply_date),
            u"product":self.product,
            u"brand_name": str(self.brand_name),
            u"i18n_type": int(self.i18n_type),
            u"brand_status": int(self.brand_status)
        }
        return str(brand_dict)

    def __dir__(self):
        brand_dict = {
            u"apply_date": str(self.apply_date),
            u"product": self.product,
            u"brand_name": str(self.brand_name),
            u"i18n_type": int(self.i18n_type),
            u"brand_status": int(self.brand_status)
        }
        return str(brand_dict)

    def create_Group_Table(self, group_no, delete):
        ###创建表
        Base_model = declarative_base()
        class brand_record_group(BrandRecord, Base_model):
            __tablename__ = 'brand_record_' + str(group_no)

        if not db_engine.dialect.has_table(db_engine, 'brand_record_' + str(group_no)):
            Base_model.metadata.create_all(bind=db_engine)
            # check table exists
            #ins = inspect(db_engine)
            # for _t in ins.get_table_names(): print _t
        elif delete == True:  # 已存在，则清空
            db_session.query(brand_record_group).delete()
            db_session.commit()
        return brand_record_group

    def get_Group_Table(self, delete=True):
        for i in range(1, 46):
            self.group_table_dict[i] = self.create_Group_Table(i, delete)
        return self.group_table_dict

    def drop_Group_Table(self, group_no, delete):
        ###创建表
        Base_model = declarative_base()
        class brand_record_group(BrandRecord, Base_model):
            __tablename__ = 'brand_record_' + str(group_no)

        if not db_engine.dialect.has_table(db_engine, 'brand_record_' + str(group_no)):
            Base_model.metadata.create_all(bind=db_engine)
            # check table exists
            #ins = inspect(db_engine)
            # for _t in ins.get_table_names(): print _t
        elif delete == True:  # 已存在，则清空
            db_session.query(brand_record_group).delete()
            db_session.query(brand_record_group).drop()
            db_session.commit()
        return brand_record_group

if __name__=='__main__':
    brand_record_group = BrandRecord()
    for i in range(1, 46):
        brand_record_group.group_table_dict[i] = brand_record_group.create_Group_Table(i, True)
