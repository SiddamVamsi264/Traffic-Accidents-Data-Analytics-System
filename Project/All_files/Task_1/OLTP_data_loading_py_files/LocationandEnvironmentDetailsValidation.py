import urllib
import datetime
import pandas as pd
import time
# Import important sqlalchemy classes
#
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float,Time,Date,Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.util import deprecations
#
#Connection string for sqlalchemy
connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-B8D6R50\\SQLEXPRESS;Database=TermProject;Trusted_Connection=yes;"
#connection_string ="Driver={ODBC Driver 17 for SQL Server};Server=stwssbsql01.ad.okstate.edu;Database=MYStoreDW;Trusted_Connection=yes;"
connection_string = urllib.parse.quote_plus(connection_string)
connection_string = "mssql+pyodbc:///?odbc_connect=%s" % connection_string
#
# Define the Base for the declarative mapping
Base = declarative_base()


class LocationAndEnvironmentDetails(Base):
    __tablename__ = 'LocationAndEnvironmentDetails'
    Accident_Index = Column(String(50), primary_key=True)
    Latitude = Column(Float, nullable=True)
    Longitude = Column(Float, nullable=True)
    Location_Easting_OSGR = Column(Integer, nullable=True)
    Location_Northing_OSGR = Column(Integer, nullable=True)
    LSOA_of_Accident_Location = Column(String(100), nullable=True)
    Light_Conditions = Column(String(100), nullable=True)
    Weather_Conditions = Column(String(100), nullable=True)
    Road_Surface_Conditions = Column(String(100), nullable=True)
    Carriageway_Hazards = Column(String(100), nullable=True)
    Special_Conditions_at_Site = Column(String(100), nullable=True)

def load_location_and_environment_details(csv_path, conn_str):
    engine = create_engine(conn_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    counter = 0

    # Read data from CSV file
    df = pd.read_csv(csv_path)
    # Extract necessary columns
    df = df[['Accident_Index', 'Latitude', 'Longitude', 'Location_Easting_OSGR', 'Location_Northing_OSGR',
             'LSOA_of_Accident_Location', 'Light_Conditions', 'Weather_Conditions',
             'Road_Surface_Conditions', 'Carriageway_Hazards', 'Special_Conditions_at_Site']]

    # Load data into the LocationAndEnvironmentDetails table
    for index, row in df.iterrows():

        if (row['Weather_Conditions'] not in (
        'Fine no high winds', 'Raining no high winds', 'Snowing no high winds', 'Fine + high winds',
        'Raining + high winds', 'Snowing + high winds', 'Fog or mist', 'Other', 'Unknown',
        'Data missing or out of range')):
            log_message(index, "Invalid Weather Conditions Data in the csv file")
            continue
        if (row['Light_Conditions'] not in (
        'Daylight', 'Darkness - lights lit', 'Darkness - no lighting', 'Darkness - lighting unknown',
        'Darkness - lights unlit', 'Data missing or out of range')):
            log_message(index, "Invalid Light Conditions in the csv file")
            continue
        if (row['Road_Surface_Conditions'] not in (
        'Dry', 'Oil or diesel', 'Mud','Wet or damp', 'Frost or ice', 'Flood over 3cm. deep', 'Snow',
        'Data missing or out of range')):
            log_message(index, "Invalid Road Surface conditions in the csv file")
            continue

        try:
            new_record = LocationAndEnvironmentDetails(
                Accident_Index=row['Accident_Index'],
                Latitude=float(row['Latitude']) if pd.notnull(row['Latitude']) else None,
                Longitude=float(row['Longitude']) if pd.notnull(row['Longitude']) else None,
                Location_Easting_OSGR=int(row['Location_Easting_OSGR']) if pd.notnull(row['Location_Easting_OSGR']) else None,
                Location_Northing_OSGR=int(row['Location_Northing_OSGR']) if pd.notnull(row['Location_Northing_OSGR']) else None,
                LSOA_of_Accident_Location=row['LSOA_of_Accident_Location'].strip() if pd.notnull(row['LSOA_of_Accident_Location']) else None,
                Light_Conditions=row['Light_Conditions'].strip() if pd.notnull(row['Light_Conditions']) else None,
                Weather_Conditions=row['Weather_Conditions'].strip() if pd.notnull(row['Weather_Conditions']) else None,
                Road_Surface_Conditions=row['Road_Surface_Conditions'].strip() if pd.notnull(row['Road_Surface_Conditions']) else None,
                Carriageway_Hazards=row['Carriageway_Hazards'].strip() if pd.notnull(row['Carriageway_Hazards']) else None,
                Special_Conditions_at_Site=row['Special_Conditions_at_Site'].strip() if pd.notnull(row['Special_Conditions_at_Site']) else None
            )
            session.add(new_record)
            counter += 1
            session.commit()
        except Exception as e:
            log_message(index, f"Error with row : {e}")
            session.rollback()
            print(row)


    session.close()

    log_summary(counter)

def log_summary(records_loaded):
    with open('location_environment_validation_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f"TimeStamp: {dt_str} --- Number of records loaded into LocationAndEnvironmentDetails = {records_loaded}\n"
        f.write(out_str)

def log_message(index,message):
    with open('location_environment_validation_log.txt', 'a') as f:
        f.write(f"Record {index}: {message}\n")

# Specify the path to the CSV file
csv_file_path = 'Accidents_extract.csv'

# Call the function to load data
load_location_and_environment_details(csv_file_path, connection_string)
