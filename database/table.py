from database.db import base
from sqlalchemy import Column,String,LargeBinary,Integer
from sqlalchemy.sql.sqltypes import TIMESTAMP
from sqlalchemy.sql.expression import text

class valid_files(base):
    __tablename__ = "validFiles"
    id = Column(Integer,primary_key=True,nullable=False)
    file = Column(LargeBinary,nullable=True)
    file_name = Column(String,nullable=True)
    time = Column(TIMESTAMP(timezone=True), nullable =True,server_default=text('now()'))