import pandas as pd
import os
import pyodbc
import urllib
import datetime

# Import important sqlalchemy classes
#
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float,Date,Time, Boolean,ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
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
def pipe_RoadConditions(conStrdb, conStrdw):
    # Connect to the source database
    engine_db = create_engine(conStrdb)
    engine_dw = create_engine(conStrdw)

    # Query the data from the source table
    sqlQuery1 = "SELECT Date_ID FROM DateDimension;"
    sqlQuery2 = "SELECT Location_ID FROM LocationDimension;"
    sqlQuery3 = "SELECT Driver_ID FROM DriversDimension;"
    sqlQuery4 = "SELECT Vehicle_ID  FROM VehicleDimension;"
    sqlQuery5 = "SELECT RoadCondition_ID FROM RoadConditionDimension;"
    sqlQuery6 = "SELECT Number_of_Casualties, Number_of_Vehicles FROM AccidentCoreDetails;"
    sqlQuery7 = "SELECT Speed_limit FROM RoadAndTrafficManagementDetails;"
    sqlQuery8 ="SELECT Engine_Capacity_CC,Accident_Index,Age_of_Vehicle FROM VehicleDetails;"
    sqlQuery9 = "SELECT Driver_IMB_Decile FROM DriverDetails;"




    dFrame1 = pd.read_sql_query(sqlQuery1, engine_dw)
    dFrame2 = pd.read_sql_query(sqlQuery2, engine_dw)
    dFrame3 = pd.read_sql_query(sqlQuery3, engine_dw)
    dFrame4 = pd.read_sql_query(sqlQuery4, engine_dw)
    dFrame5 = pd.read_sql_query(sqlQuery5, engine_dw)
    dFrame6 = pd.read_sql_query(sqlQuery6, engine_db)
    dFrame7 = pd.read_sql_query(sqlQuery7, engine_db)
    dFrame8 = pd.read_sql_query(sqlQuery8, engine_db)
    dFrame9 = pd.read_sql_query(sqlQuery9, engine_db)


    dFrame = pd.concat([dFrame1, dFrame2, dFrame3,dFrame4,dFrame5,dFrame6,dFrame7,dFrame8,dFrame9], axis=1)
    # Define the base class for the ORM
    Base = declarative_base()

    class LocationDim(Base):
        __tablename__ = 'LocationDimension'
        Location_ID = Column(Integer, primary_key=True)

    class DriversDim(Base):
        __tablename__ = 'DriversDimension'
        Driver_ID = Column(Integer, primary_key=True)

    class DateDim(Base):
        __tablename__ = 'DateDimension'
        Date_ID= Column(Integer, primary_key=True)

    class VehicleDim(Base):
        __tablename__ = 'VehicleDimension'
        Vehicle_ID = Column(Integer, primary_key=True)

    class RoadConditionDim(Base):
        __tablename__ = 'RoadConditionDimension'
        RoadCondition_ID = Column(Integer, primary_key=True)

    # Define the RoadConditionDimension class
    class DriverVehicleFact(Base):
        __tablename__ = 'AccidentVehicleFact'
        Fact_Id = Column(Integer, primary_key=True, autoincrement=True)
        Accident_Index = Column(String(50))
        Date_ID = Column(Integer, ForeignKey('DateDimension.Date_ID'))
        Location_ID = Column(Integer, ForeignKey('LocationDimension.Location_ID'))
        Driver_ID = Column(Integer, ForeignKey('DriversDimension.Driver_ID'))
        Vehicle_ID = Column(Integer, ForeignKey('VehicleDimension.Vehicle_ID'))
        RoadCondition_ID = Column(Integer, ForeignKey('RoadConditionDimension.RoadCondition_ID'))
        Number_of_Casualties = Column(Integer)
        Number_of_Vehicles = Column(Integer)
        Speed_limit = Column(Integer)
        Age_of_Vehicle = Column(Integer)
        Driver_IMB_Decile = Column(Integer)
        Engine_Capacity_CC = Column(Integer)

    location_fact =relationship("LocationDim")
    driver_fact = relationship("DriversDim")
    date_fact = relationship("DateDim")
    vehicle_fact= relationship("VehicleDim")
    road_fact = relationship("RoadConditionDim")




    # Connect to the destination database
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

# Iterate over the DataFrame and insert records into the destination table
    for _, row in dFrame.iterrows():

        record = {key: None if pd.isna(value) else value for key, value in row.to_dict().items()}

        new_record = DriverVehicleFact(**record)
        session.add(new_record)
        counter += 1

    # Commit the transaction and close the session
        session.commit()
    session.close()

    # Logging
    log_activity(counter)


# Define a function to log activity
def log_activity(records_loaded):
    with open('Driver_Vehicle_Fact_log.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"TimeStamp: {current_time} --- Number of records loaded into DriverVehicleFact: {records_loaded}\n"
        log_file.write(log_message)


# Call the function to start the data pipeline
pipe_RoadConditions(conn_string_db,conn_string_dw)