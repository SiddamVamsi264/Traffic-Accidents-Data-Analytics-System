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

# Define the Base for the declarative mapping
Base = declarative_base()

class AccidentCoreDetails(Base):
    __tablename__ = 'AccidentCoreDetails'
    Accident_Index = Column(String(50), primary_key=True)
    Date = Column(Date, nullable=True)
    Day_of_Week = Column(String(50), nullable=True)
    Time = Column(Time, nullable=True)
    Accident_Severity = Column(String(50), nullable=True)
    Number_of_Casualties = Column(Integer, nullable=True)
    Number_of_Vehicles = Column(Integer, nullable=True)
    Did_Police_Officer_Attend_Scene_of_Accident = Column(Integer, nullable=True)
    Year = Column(Integer, nullable=True)
    Urban_or_Rural_Area = Column(String(50), nullable=True)
    InScotland = Column(Boolean, nullable=True)

def load_accident_data(csv_path,conn_str):
    engine = create_engine(conn_str)
    Session = sessionmaker(bind=engine)
    session = Session()
    counter = 0

    # Read data from CSV file
    df = pd.read_csv(csv_path)
    # Extract necessary columns
    df = df[['Accident_Index', 'Date', 'Day_of_Week', 'Time', 'Accident_Severity',
             'Number_of_Casualties', 'Number_of_Vehicles', 'Did_Police_Officer_Attend_Scene_of_Accident',
             'Year', 'Urban_or_Rural_Area', 'InScotland']]
    Session = sessionmaker(bind=engine)
    session = Session()

    for index, row in df.iterrows():
        #Validation
        if(row['Urban_or_Rural_Area'] not in ('Urban','Rural','Unallocated','Data missing or out of range')):
            log_message(index,"Invalid Urban or Rural Area Data in the csv file")
            continue
        if(row['Accident_Severity'] not in ('Fatal','Serious','Slight')):
            log_message(index, "Invalid Accident severity in the csv file")
            continue

        if( row['Did_Police_Officer_Attend_Scene_of_Accident'] < 1 or row['Did_Police_Officer_Attend_Scene_of_Accident'] > 3 ):
            if(row['Did_Police_Officer_Attend_Scene_of_Accident'] is None):
                log_message(index, "Data missing or out of range in the column 'Did_Police_Officer_Attend_Scene_of_Accident'  ")
            else:
                log_message(index,"Invalid input for at {index} and skipping the row in the csv")
                continue



        try:
            new_record = AccidentCoreDetails(
                Accident_Index=row['Accident_Index'],
                Date=pd.to_datetime(row['Date']).date() if pd.notnull(row['Date']) else None,
                Day_of_Week=row['Day_of_Week'].strip() if pd.notnull(row['Day_of_Week']) else None,
                Time=pd.to_datetime(row['Time'], format='%H:%M').time() if pd.notnull(row['Time']) else None,
                Accident_Severity=row['Accident_Severity'].strip() if pd.notnull(row['Accident_Severity']) else None,
                Number_of_Casualties=int(row['Number_of_Casualties']) if pd.notnull(row['Number_of_Casualties']) else None,
                Number_of_Vehicles=int(row['Number_of_Vehicles']) if pd.notnull(row['Number_of_Vehicles']) else None,
                Did_Police_Officer_Attend_Scene_of_Accident=int(row['Did_Police_Officer_Attend_Scene_of_Accident']) if pd.notnull(row['Did_Police_Officer_Attend_Scene_of_Accident']) else -1,
                Year=int(row['Year']) if pd.notnull(row['Year']) else None,
                Urban_or_Rural_Area=row['Urban_or_Rural_Area'].strip() if pd.notnull(row['Urban_or_Rural_Area']) else None,
                InScotland = True if row['InScotland'] == 'Yes' or row['InScotland'] == 1 else False if row['InScotland'] == 'No' or row['InScotland'] == 0 else None
            )
            session.add(new_record)
            counter += 1

            session.commit()
        except Exception as e:

            log_message(index,f"Error with row : {e}")
            session.rollback()
            print(row)

        # Close the session
    session.close()

    log_summary(counter)


def log_summary(records_loaded):
    with open('accident_validation_details_log.txt', 'a') as f:
        dt = datetime.datetime.now()
        dt_str = dt.strftime("%Y-%m-%d %H:%M:%S")
        out_str = f"TimeStamp: {dt_str} --- Number of records loaded into AccidentCoreDetails = {records_loaded}\n"
        f.write(out_str)

def log_message(index,message):
    with open('accident_validation_details_log.txt', 'a') as f:
        f.write(f"Record {index}: {message}\n")

# Specify the path to the CSV file
csv_file_path = 'Accidents_extract.csv'

# Call the function to load data
load_accident_data(csv_file_path, connection_string)