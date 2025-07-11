import pandas as pd
import os
import pyodbc
import urllib
import datetime

# Import important sqlalchemy classes
#
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float,Date,Time, Boolean
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


def pipe_VehicleDimension(conStrdb, conStrdw):
    # Connect to the source database
    engine_db = create_engine(conStrdb)

    # Query the data from the source table
    #qlQuery1 = "SELECT First_Road_Class, Second_Road_Class, Junction_Control, Junction_Detail, Pedestrian_Crossing_Human_Control, Pedestrian_Crossing_Physical_Facilities, Road_Type FROM RoadAndTrafficManagementDetails;"

    sqlQuery1 = "SELECT Vehicle_Location_Restricted_Lane, Make, Model, Propulsion_Code, Skidding_and_Overturning,Towing_and_Articulation,Vehicle_Leaving_Carriageway, Vehicle_Manoeuvre, Vehicle_Type, Was_Vehicle_Left_Hand_Drive, X1st_Point_of_Impact,Hit_Object_in_Carriageway,Hit_Object_off_Carriageway,Junction_Location  FROM VehicleDetails;"


    dFrame1 = pd.read_sql_query(sqlQuery1, engine_db)


    dFrame = pd.concat([dFrame1], axis=1)
    # Define the base class for the ORM
    Base = declarative_base()

    class VehicleDim(Base):
        __tablename__ = 'VehicleDimension'
        Vehicle_ID = Column(Integer, primary_key=True,autoincrement=True)
        Vehicle_Location_Restricted_Lane = Column(String(50))
        Make = Column(String(50))
        Model = Column(String(50))
        Propulsion_Code = Column(String(50))
        Skidding_and_Overturning = Column(String(50))
        Towing_and_Articulation = Column(String(50))
        Vehicle_Leaving_Carriageway = Column(String(50))
        Vehicle_Manoeuvre = Column(String(100))
        Vehicle_Type = Column(String(50))
        Was_Vehicle_Left_Hand_Drive = Column(String(10))
        X1st_Point_of_Impact = Column(String(50))
        Hit_Object_in_Carriageway = Column(String(100))
        Hit_Object_off_Carriageway = Column(String(100))
        Junction_Location = Column(String(100))

        # Connect to the destination database

    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    # Iterate over the DataFrame and insert records into the destination table
    for _, row in dFrame.iterrows():
        record = {key: None if pd.isna(value) else value for key, value in row.to_dict().items()}

        new_record = VehicleDim(**record)
        session.add(new_record)
        counter += 1

        # Commit the transaction and close the session
        session.commit()
    session.close()

    # Logging
    log_activity(counter)


# Define a function to log activity
def log_activity(records_loaded):
    with open('pipe_Vehicle_Dimension_log.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"TimeStamp: {current_time} --- Number of records loaded into DriverDimension: {records_loaded}\n"
        log_file.write(log_message)


# Call the function to start the data pipeline
pipe_VehicleDimension(conn_string_db, conn_string_dw)