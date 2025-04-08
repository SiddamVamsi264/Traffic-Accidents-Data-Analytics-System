import urllib
import datetime
import pandas as pd
import time
# Import important sqlalchemy classes
#
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Time, Date, Boolean, Identity
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

class DriverDetails(Base):
    __tablename__ = 'DriverDetails'
    Driver_ID = Column(Integer, Identity(start=1, cycle=False), primary_key=True)
    Accident_Index = Column(String(50))
    Age_Band_of_Driver = Column(String(50))
    Driver_Home_Area_Type = Column(String(50))
    Driver_IMD_Decile = Column(Integer)
    Sex_of_Driver = Column(String(10))
    Journey_Purpose_of_Driver = Column(String(255))

def load_driver_details(csv_path, connection_string):
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    counter = 0
    df = pd.read_csv(csv_path)

    df = df[['Accident_Index', 'Age_Band_of_Driver','Driver_Home_Area_Type','Driver_IMD_Decile','Sex_of_Driver','Journey_Purpose_of_Driver']]

    for index, row in df.iterrows():

        if row['Sex_of_Driver'] not in ('Male', 'Female', 'Not known', 'Data missing or out of range'):
            log_message(index, "Invalid Sex_of_Driver data.")
            continue
        if row['Age_Band_of_Driver'] not in ('Data missing or out of range', '0 - 5', '6 - 10', '11 - 15', '16 - 20', '21 - 25', '26 - 35', '36 - 45',
        '46 - 55', '56 - 65', '66 - 75','Over 75'):
            log_message(index, "Invalid Age Band of Driver data.")
            continue
        if (row['Journey_Purpose_of_Driver'] not in ('Journey as part of work', 'Commuting to/from work', 'Taking pupil to/from school',
                'Pupil riding to/from school', 'Other', 'Not known', 'Other/Not known (2005-10)','Data missing or out of range')):
            log_message(index, "Invalid Journey of purpose data in the csv file")
            continue

        try:
            sex = row['Sex_of_Driver'].strip() if pd.notnull(row['Sex_of_Driver']) and row['Sex_of_Driver'].strip() in ['Male', 'Female'] else 'Unknown'
            driver = DriverDetails(
                Accident_Index=row['Accident_Index'],
                Age_Band_of_Driver=row['Age_Band_of_Driver'].strip() if pd.notnull(row['Age_Band_of_Driver']) else None,
                Driver_Home_Area_Type=row['Driver_Home_Area_Type'].strip() if pd.notnull(row['Driver_Home_Area_Type']) else None,
                Driver_IMD_Decile=int(row['Driver_IMD_Decile']) if pd.notnull(row['Driver_IMD_Decile'])else None,
                Sex_of_Driver=sex,
                Journey_Purpose_of_Driver=row['Journey_Purpose_of_Driver'].strip() if pd.notnull(row['Journey_Purpose_of_Driver']) else None
            )
            session.add(driver)
            counter += 1
            session.commit()

        except Exception as e:

            log_message(index, f"Error with row : {e}")
            session.rollback()
            print(row)


    session.close()
    log_summary(counter)

def log_summary(records_loaded):
    with open('driver_details_log.txt', 'a') as f:
        dt_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"Timestamp: {dt_str} --- Number of records loaded into DriverDetails = {records_loaded}\n")

def log_message(index,message):
    with open('driver_details_log.txt', 'a') as f:
        f.write(f"Record {index}: {message}\n")

# Specify the path to the CSV file
csv_file_path = 'Vehicles_extract.csv'

# Call the function to load data
load_driver_details(csv_file_path, connection_string)
