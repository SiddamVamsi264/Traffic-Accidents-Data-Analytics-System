import urllib
import datetime
import pandas as pd
import time
# Import important sqlalchemy classes
#
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Time, Date, Boolean, ForeignKey,Identity
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
    Driver_ID = Column(Integer, primary_key=True)
    Accident_Index = Column(String(50))

class VehicleDetails(Base):
    __tablename__ = 'VehicleDetails'
    Vehicle_ID = Column(Integer,  Identity(start=1, cycle=False),primary_key=True)
    Driver_ID = Column(Integer, ForeignKey('DriverDetails.Driver_ID'))  # Assuming DriverDetails is loaded correctly
    Accident_Index = Column(String(50))
    Age_of_Vehicle = Column(Integer)
    Engine_Capacity_CC = Column(Integer)
    Make = Column(String(50))
    Model = Column(String(50))
    Propulsion_Code = Column(String(50))
    Vehicle_Type = Column(String(50))
    Vehicle_Manoeuvre = Column(String(100))
    X1st_Point_of_Impact = Column(String(50))
    Skidding_and_Overturning = Column(String(50))
    Towing_and_Articulation = Column(String(50))
    Vehicle_Leaving_Carriageway = Column(String(50))
    Vehicle_Location_Restricted_Lane = Column(String(50))
    Hit_Object_in_Carriageway = Column(String(100))
    Hit_Object_off_Carriageway = Column(String(100))
    Junction_Location = Column(String(100))
    Was_Vehicle_Left_Hand_Drive = Column(String(10))
    Year = Column(Integer)

def load_vehicle_details(csv_path, connection_string):
    engine = create_engine(connection_string)
    Session = sessionmaker(bind=engine)
    session = Session()
    df = pd.read_csv(csv_path)

    df = df[['Accident_Index', 'Age_of_Vehicle', 'Engine_Capacity_.CC.', 'make', 'model','Propulsion_Code','Vehicle_Type','Vehicle_Manoeuvre','X1st_Point_of_Impact',
             'Skidding_and_Overturning','Towing_and_Articulation','Vehicle_Leaving_Carriageway',
             'Vehicle_Location.Restricted_Lane','Hit_Object_in_Carriageway','Hit_Object_off_Carriageway',
             'Junction_Location','Was_Vehicle_Left_Hand_Drive','Year']]

    counter = 0

    for index, row in df.iterrows():

        # Validation

        if (row['Towing_and_Articulation'] not in ('No tow/articulation', 'Articulated vehicle', 'Double or multiple trailer','Single trailer','Other tow','Data missing or out of range','unknown (self reported)','Caravan')):
            log_message(index, "Invalid Towing and Articulation Data in the csv file")
            continue
        if (row['Vehicle_Leaving_Carriageway'] not in ('Data missing or out of range','Did not leave carriageway','Nearside','Nearside and rebounded', 'Straight ahead at junction','Offside on to central reservation','Offside on to centrl res + rebounded','Offside - crossed central reservation',
        'Offside','Offside and rebounded','unknown (self reported)')):
            log_message(index, "Invalid Vehicle leaving carriageway data in the csv file")
            continue
        if (row['Was_Vehicle_Left_Hand_Drive'] not in ('No','Yes','Unknown','Data missing or out of range')):
            log_message(index, "Invalid Was Vehicle left hand drive data in the csv file")
            continue

        try:
            drivers = session.query(DriverDetails).filter(DriverDetails.Accident_Index == row['Accident_Index']).all()
            if drivers:
                for driver in drivers:
                    left_hand_drive = row['Was_Vehicle_Left_Hand_Drive'].strip()
                    if left_hand_drive not in ['Yes', 'No']:
                        left_hand_drive = 'Unknown'
                    vehicle = VehicleDetails(
                        Driver_ID=driver.Driver_ID,  # Ensure this ID is linked properly from DriverDetails
                        Accident_Index=row['Accident_Index'].strip() if pd.notnull(row['Accident_Index']) else None,
                        Age_of_Vehicle=int(row['Age_of_Vehicle']) if pd.notnull(row['Age_of_Vehicle']) else None,
                        Engine_Capacity_CC=int(row['Engine_Capacity_.CC.']) if pd.notnull(row['Engine_Capacity_.CC.']) else None,
                        Make=row['make'].strip() if pd.notnull(row['make']) else None,
                        Model=row['model'].strip() if pd.notnull(row['model']) else None,
                        Propulsion_Code=row['Propulsion_Code'].strip() if pd.notnull(row['Propulsion_Code']) else None,
                        Vehicle_Type=row['Vehicle_Type'].strip() if pd.notnull(row['Vehicle_Type']) else None,
                        Vehicle_Manoeuvre=row['Vehicle_Manoeuvre'].strip() if pd.notnull(row['Vehicle_Manoeuvre']) else None,
                        X1st_Point_of_Impact=row['X1st_Point_of_Impact'].strip() if pd.notnull(row['X1st_Point_of_Impact']) else None,
                        Skidding_and_Overturning=row['Skidding_and_Overturning'].strip() if pd.notnull(row['Skidding_and_Overturning']) else None,
                        Towing_and_Articulation=row['Towing_and_Articulation'].strip() if pd.notnull(row['Towing_and_Articulation']) else None,
                        Vehicle_Leaving_Carriageway=row['Vehicle_Leaving_Carriageway'].strip() if pd.notnull(row['Vehicle_Leaving_Carriageway']) else None,
                        Vehicle_Location_Restricted_Lane=row['Vehicle_Location.Restricted_Lane'] if pd.notnull(row['Vehicle_Location.Restricted_Lane']) else None,
                        Hit_Object_in_Carriageway=row['Hit_Object_in_Carriageway'].strip() if pd.notnull(row['Hit_Object_in_Carriageway']) else None,
                        Hit_Object_off_Carriageway=row['Hit_Object_off_Carriageway'].strip() if pd.notnull(row['Hit_Object_off_Carriageway']) else None,
                        Junction_Location=row['Junction_Location'].strip() if pd.notnull(row['Junction_Location']) else None,
                        Was_Vehicle_Left_Hand_Drive=left_hand_drive,
                        Year=int(row['Year']) if pd.notnull(row['Year']) else None
                    )
                    session.add(vehicle)
                    counter += 1
                    session.commit()
            else:
                log_message(f"No Driver found with Accident Index: {row['Accident_Index']} at row {index}")
                print(f"No Driver found with Accident Index: {row['Accident_Index']} at row {index}")

        except Exception as e:

            log_message(index, f"Error with row : {e}")
            session.rollback()
            print(row)

    session.close()
    log_summary(counter)
def log_summary(records_loaded):
    with open('vehicle_details_validation_log.txt', 'a') as f:
        dt_str = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        f.write(f"Timestamp: {dt_str} --- Number of records loaded into DriverDetails = {records_loaded}\n")

def log_message(index,message):
    with open('vehicle_details_validation_log.txt', 'a') as f:
        f.write(f"Record {index}: {message}\n")

csv_file_path = 'Vehicles_extract.csv'

# Call the function to load data
load_vehicle_details(csv_file_path, connection_string)



