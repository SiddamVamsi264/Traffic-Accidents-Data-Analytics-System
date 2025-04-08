import pandas as pd
import os
import pyodbc
import urllib
import datetime
# Import important sqlalchemy classes
#
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float,Date,Time
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.util import deprecations
deprecations.SILENCE_UBER_WARNING = True
#
# Define the connection strings to the MyStore and MyStoreDWETL databases
conn_string_db = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-B8D6R50\\SQLEXPRESS;Database=TermProject;Trusted_Connection=yes;"
conn_string_db = urllib.parse.quote_plus(conn_string_db)
conn_string_db = "mssql+pyodbc:///?odbc_connect=%s" % conn_string_db
conn_string_dw = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-B8D6R50\\SQLEXPRESS;Database=TermProjectDW;Trusted_Connection=yes;"
conn_string_dw = urllib.parse.quote_plus(conn_string_dw)
conn_string_dw = "mssql+pyodbc:///?odbc_connect=%s" % conn_string_dw
#####################################################################################
def pipe_Customer(conStrdb, conStrdw):
    engine_db = create_engine(conStrdb)
    sqlQuery = f'SELECT Date,Day_of_Week,Year,Time FROM AccidentCoreDetails;'
    dFrame = pd.read_sql_query(sqlQuery, engine_db)

    Base = declarative_base()
    class DateDim(Base):
        __tablename__ = 'DateDimension'
        Date_ID= Column(Integer, primary_key=True, autoincrement=True)
        Date = Column(Date)
        Day_of_Week = Column(String)
        Year=Column(Integer)
        Time =Column(Time)

    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    for _, row in dFrame.iterrows():
        new_record = DateDim(
            Date=row['Date'],
            Day_of_Week=row['Day_of_Week'],
            Year=row['Year'],
            Time=row['Time']
        )

        session.add(new_record)
        counter += 1

        session.commit()
    session.close()

    # Logging
    log_activity(counter)


def log_activity(records_loaded):
    with open('date_dimension_log.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"TimeStamp: {current_time} --- Number of records loaded into DateDimension: {records_loaded}\n"
        log_file.write(log_message)

pipe_Customer(conn_string_db, conn_string_dw)