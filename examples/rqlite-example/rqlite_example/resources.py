from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

import kstreams

stream_engine = kstreams.create_engine(title="my-stream-engine")

db_engine = create_engine("rqlite+pyrqlite://localhost:4001/", echo=True)
Session = sessionmaker(autocommit=False, autoflush=False, bind=db_engine)
Base = declarative_base()
