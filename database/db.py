from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from validifi.config import db_url



engine = create_engine(db_url)

session = sessionmaker(autocommit = False , autoflush = False,bind = engine)

base = declarative_base()


def db():
    db = session()
    try:
        yield db
    finally:
        db.close()
