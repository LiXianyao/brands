from sqlalchemy import create_engine, MetaData, inspect
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy.ext.declarative import declarative_base

db_engine = create_engine('mysql://root:123456@10.109.246.96:3306/Brand?charset=utf8', convert_unicode=True)
ins = inspect(db_engine)
db_metadata = MetaData(db_engine,reflect=True)
db_inspector = inspect(db_engine)
db_session = scoped_session(sessionmaker(autocommit=False,
                                         autoflush=False,
                                         bind=db_engine))

Base = declarative_base()
Base.query = db_session.query_property()

def init_db():
    Base.metadata.create_all(bind=db_engine)