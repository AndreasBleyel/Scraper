from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine

Base = declarative_base()

class Event(Base):
    __tablename__ = 'event'

    id = Column(Integer, primary_key=True)
    identifier = Column(String)
    name = Column(String)
    location = Column(String)
    source = Column(String)
    short = Column(String)
    date = Column(DateTime)
    link = Column(String)


if __name__ == '__main__':
    engine = create_engine('sqlite:///events_db.sqlite')
    Base.metadata.create_all(engine)