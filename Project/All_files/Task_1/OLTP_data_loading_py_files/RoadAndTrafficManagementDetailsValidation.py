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


class RoadAndTrafficManagementDetails(Base):
    __tablename__ = 'RoadAndTrafficManagementDetails'
    Accident_Index = Column(String(50), primary_key=True)
    First_Road_Class = Column(String(50), nullable=True)
    First_Road_Number = Column(Integer, nullable=True)
    Second_Road_Class = Column(String(50), nullable=True)
    Second_Road_Number = Column(Integer, nullable=True)
    Road_Type = Column(String(50), nullable=True)
    Speed_limit = Column(Integer, nullable=True)
    Junction_Control = Column(String(100), nullable=True)
    Junction_Detail = Column(String(100), nullable=True)
    Pedestrian_Crossing_Human_Control = Column(Integer, nullable=True)
    Pedestrian_Crossing_Physical_Facilities = Column(Integer, nullable=True)
    Local_Authority_District = Column(String(100), nullable=True)
    Local_Authority_Highway = Column(String(100), nullable=True)
    Police_Force = Column(String(100), nullable=True)

def load_road_traffic_management_details(csv_path, conn_str):
    engine = create_engine(conn_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    counter = 0

    # Read data from CSV file
    df = pd.read_csv(csv_path)
    # Extract necessary columns
    df = df[['Accident_Index', '1st_Road_Class', '1st_Road_Number', '2nd_Road_Class', '2nd_Road_Number',
             'Road_Type', 'Speed_limit', 'Junction_Control', 'Junction_Detail',
             'Pedestrian_Crossing-Human_Control', 'Pedestrian_Crossing-Physical_Facilities',
             'Local_Authority_(District)', 'Local_Authority_(Highway)', 'Police_Force']]

    # Load data into the RoadAndTrafficManagementDetails table
    for index, row in df.iterrows():

        # Validation

        if (row['Junction_Detail'] not in ('Not at junction or within 20 metres','Roundabout','Mini-roundabout','T or staggered junction',
            'Slip road','Crossroads','More than 4 arms (not roundabout)','Private drive or entrance','Other junction','unknown (self reported)','Data missing or out of range')):
            log_message(index, "Invalid Junction Details Data in the csv file")
            continue
        if (row['1st_Road_Class'] not in ('Motorway', 'A(M)', 'A', 'B', 'C', 'Unclassified')):
            log_message(index, "Invalid First_road_class data in the csv file")
            continue
        if (row['Junction_Control'] not in ('Not at junction or within 20 metres','Authorised person','Auto traffic signal','Stop sign','Give way or uncontrolled',
            'Data missing or out of range','Unknown (self reported)')):

            log_message(index, "Invalid Junction control data in the csv file")
            continue

        try:

            new_record = RoadAndTrafficManagementDetails(
                Accident_Index=row['Accident_Index'],
                First_Road_Class=row['1st_Road_Class'].strip() if pd.notnull(row['1st_Road_Class']) else None,
                First_Road_Number=int(row['1st_Road_Number']) if pd.notnull(row['1st_Road_Number']) else None,
                Second_Road_Class=row['2nd_Road_Class'].strip() if pd.notnull(row['2nd_Road_Class']) else None,
                Second_Road_Number=int(row['2nd_Road_Number']) if pd.notnull(row['2nd_Road_Number']) else None,
                Road_Type=row['Road_Type'].strip() if pd.notnull(row['Road_Type']) else None,
                Speed_limit=int(row['Speed_limit']) if pd.notnull(row['Speed_limit']) else None,
                Junction_Control=row['Junction_Control'].strip() if pd.notnull(row['Junction_Control']) else None,
                Junction_Detail=row['Junction_Detail'].strip() if pd.notnull(row['Junction_Detail']) else None,
                Pedestrian_Crossing_Human_Control=int(row['Pedestrian_Crossing-Human_Control']) if pd.notnull(row['Pedestrian_Crossing-Human_Control']) else None,
                Pedestrian_Crossing_Physical_Facilities=int(row['Pedestrian_Crossing-Physical_Facilities']) if pd.notnull(row['Pedestrian_Crossing-Physical_Facilities']) else None,
                Local_Authority_District=row['Local_Authority_(District)'].strip() if pd.notnull(row['Local_Authority_(District)']) else None,
                Local_Authority_Highway=row['Local_Authority_(Highway)'].strip() if pd.notnull(row['Local_Authority_(Highway)']) else None,
                Police_Force=row['Police_Force'].strip() if pd.notnull(row['Police_Force']) else None
            )
            session.add(new_record)
            counter += 1
            session.commit()
        except Exception as e:
            log_message(index, f"Error with row : {e}")
            session.rollback()
            print(row)
    # Close the session
    session.close()

    log_summary(counter)

def log_summary(records_loaded):
    with open('road_traffic_validation_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f"TimeStamp: {dt_str} --- Number of records loaded into RoadAndTrafficManagementDetails = {records_loaded}\n"
        f.write(out_str)

def log_message(index,message):
    with open('road_traffic_validation_log.txt', 'a') as f:
        f.write(f"Record {index}: {message}\n")

# Specify the path to the CSV file
csv_file_path = 'Accidents_extract.csv'



# Call the function to load data
load_road_traffic_management_details(csv_file_path, connection_string)
