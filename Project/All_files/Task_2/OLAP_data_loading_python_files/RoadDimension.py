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
def pipe_RoadConditions(conStrdb, conStrdw):
    # Connect to the source database
    engine_db = create_engine(conStrdb)

    # Query the data from the source table
    sqlQuery1 = "SELECT First_Road_Class, Second_Road_Class, Junction_Control, Junction_Detail, Pedestrian_Crossing_Human_Control, Pedestrian_Crossing_Physical_Facilities, Road_Type FROM RoadAndTrafficManagementDetails;"

    sqlQuery2 = "SELECT Accident_Severity, Did_Police_Officer_Attend_Scene_of_Accident FROM AccidentCoreDetails;"

    sqlQuery3 = "SELECT Carriageway_Hazards, Light_Conditions,Road_Surface_Conditions,Special_Conditions_at_Site, Weather_Conditions   FROM LocationAndEnvironmentDetails;"


    dFrame1 = pd.read_sql_query(sqlQuery1, engine_db)
    dFrame2 = pd.read_sql_query(sqlQuery2, engine_db)
    dFrame3 = pd.read_sql_query(sqlQuery3, engine_db)

    dFrame = pd.concat([dFrame1, dFrame2, dFrame3], axis=1)
    # Define the base class for the ORM
    Base = declarative_base()

    # Define the RoadConditionDimension class
    class RoadConditionDim(Base):
        __tablename__ = 'RoadConditionDimension'
        RoadCondition_ID = Column(Integer, primary_key=True, autoincrement=True)
        First_Road_Class = Column(String(50))
        Second_Road_Class = Column(String(50))
        Accident_Severity = Column(String(50))
        Carriageway_Hazards = Column(String(100))
        Did_Police_Officer_Attend_Scene_of_Accident = Column(Integer)
        Junction_Control = Column(String(100))
        Junction_Detail = Column(String(100))
        Light_Conditions = Column(String(100))
        Road_Surface_Conditions = Column(String(50))
        Pedestrian_Crossing_Human_Control = Column(Integer)
        Pedestrian_Crossing_Physical_Facilities = Column(Integer)
        Road_Type = Column(String(50))
        Special_Conditions_at_Site = Column(String(100))
        Weather_Conditions = Column(String(100))

    # Connect to the destination database
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

# Iterate over the DataFrame and insert records into the destination table
    for _, row in dFrame.iterrows():
        '''        new_record = RoadConditionDim(
            First_Road_Class=row['First_Road_Class'],
            Second_Road_Class=row['Second_Road_Class'],
            Accident_Severity=row['Accident_Severity'],
            Carriageway_Hazards=row['Carriageway_Hazards'],
            Did_Police_Officer_Attend_Scene_of_Accident=row['Did_Police_Officer_Attend_Scene_of_Accident'],
            Junction_Control=row['Junction_Control'],
            Junction_Detail=row['Junction_Detail'],
            Light_Conditions=row['Light_Conditions'],
            Road_Surface_Conditions=row['Road_Surface_Conditions'],
            Pedestrian_Crossing_Human_Control=row['Pedestrian_Crossing_Human_Control'],
            Pedestrian_Crossing_Physical_Facilities=row['Pedestrian_Crossing_Physical_Facilities'],
            Road_Type=row['Road_Type'],
            Special_Conditions_at_Site=row['Special_Conditions_at_Site'],
            Weather_Conditions=row['Weather_Conditions'],
        )
'''
        record = {key: None if pd.isna(value) else value for key, value in row.to_dict().items()}

        new_record = RoadConditionDim(**record)
        session.add(new_record)
        counter += 1

    # Commit the transaction and close the session
        session.commit()
    session.close()

    # Logging
    log_activity(counter)


# Define a function to log activity
def log_activity(records_loaded):
    with open('road_condition_dimension_log.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"TimeStamp: {current_time} --- Number of records loaded into RoadConditionDimension: {records_loaded}\n"
        log_file.write(log_message)


# Call the function to start the data pipeline
pipe_RoadConditions(conn_string_db,conn_string_dw)