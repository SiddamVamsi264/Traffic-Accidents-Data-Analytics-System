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
def pipe_Locations(conStrdb, conStrdw):
    # Connect to the source database
    engine_db = create_engine(conStrdb)

    # Query the data from the source table
    sqlQuery1 = "SELECT Accident_Index, Location_Easting_OSGR, Location_Northing_OSGR, LSOA_of_Accident_Location, Latitude, Longitude   FROM LocationAndEnvironmentDetails;"
    sqlQuery2 = "SELECT First_Road_Number, Second_Road_Number, Local_Authority_District, Local_Authority_Highway, Police_Force   FROM RoadAndTrafficManagementDetails;"
    sqlQuery3 = "SELECT Urban_or_Rural_Area, InScotland   FROM AccidentCoreDetails;"

    dFrame1 = pd.read_sql_query(sqlQuery1, engine_db)
    dFrame2 = pd.read_sql_query(sqlQuery2, engine_db)
    dFrame3 = pd.read_sql_query(sqlQuery3, engine_db)

    dFrame = pd.concat([dFrame1, dFrame2, dFrame3], axis=1)
    # Define the base class for the ORM
    Base = declarative_base()

    # Define the LocationDimension class
    class LocationDim(Base):
        __tablename__ = 'LocationDimension'
        Location_ID = Column(Integer, primary_key=True, autoincrement=True)
        Accident_Index = Column(String(50), nullable=False)
        First_Road_Number = Column(Integer)
        Second_Road_Number = Column(Integer)
        Location_Easting_OSGR = Column(Integer)
        Location_Northing_OSGR = Column(Integer)
        LSOA_of_Accident_Location = Column(String(50))
        Urban_or_Rural_Area = Column(String(50))
        Latitude = Column(Float)
        Longitude = Column(Float)
        Local_Authority_District = Column(String(100))
        Local_Authority_Highway = Column(String(100))
        Police_Force = Column(String(100))
        InScotland = Column(Boolean)

    # Connect to the destination database
    engine_dw = create_engine(conStrdw)
    Session = sessionmaker(bind=engine_dw)
    session = Session()
    counter = 0

    # Iterate over the DataFrame and insert records into the destination table
    for _, row in dFrame.iterrows():
        '''new_record = LocationDim(
            Accident_Index=row['Accident_Index'],
            First_Road_Number=row['First_Road_Number'],
            Second_Road_Number=row['Second_Road_Number'],
            Location_Easting_OSGR=row['Location_Easting_OSGR'],
            Location_Northing_OSGR=row['Location_Northing_OSGR'],
            LSOA_of_Accident_Location=row['LSOA_of_Accident_Location'],
            Urban_or_Rural_Area=row['Urban_or_Rural_Area'],
            Latitude=row['Latitude'],
            Longitude=row['Longitude'],
            Local_Authority_District=row['Local_Authority_District'],
            Local_Authority_Highway=row['Local_Authority_Highway'],
            Police_Force=row['Police_Force'],
            InScotland=row['InScotland'],
            Junction_Location=row['Junction_Location']
        )
'''
        record = {key: None if pd.isna(value) else value for key, value in row.to_dict().items()}

        new_record = LocationDim(**record)
        session.add(new_record)
        counter += 1

    # Commit the transaction and close the session
        session.commit()
    session.close()

    # Logging
    log_activity(counter)


# Define a function to log activity
def log_activity(records_loaded):
    with open('locations_dimension_log.txt', 'a') as log_file:
        current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_message = f"TimeStamp: {current_time} --- Number of records loaded into LocationDimension: {records_loaded}\n"
        log_file.write(log_message)


# Call the function to start the data pipeline
pipe_Locations(conn_string_db,conn_string_dw)