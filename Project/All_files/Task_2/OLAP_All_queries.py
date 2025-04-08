import pandas as pd
from tabulate import tabulate
import pyodbc
import os, sys

# Setup the database connection
connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-B8D6R50\\SQLEXPRESS;Database=TermProjectDW;Trusted_Connection=yes;"
cnxn = pyodbc.connect(connection_string)
cursor = cnxn.cursor()

# Function to execute queries and write results to a file
def queryResult(title, query, outFile):
    cursor.execute(query)
    col_names = [i[0] for i in cursor.description]
    result = pd.DataFrame.from_records(cursor, columns=col_names)
    with open(outFile, 'a') as f:
        print("\n\n", title, file=f)
        print("\n\n", query, file=f)
        print("\n\n", tabulate(result, headers=col_names, tablefmt="grid", showindex="always"), file=f)

# List of query titles and SQL statements
queries = [
    ("1. Accident Analysis by Year and Area", """
        SELECT 
            COALESCE(CONVERT(VARCHAR, DD.Year), 'All Years') AS Year,
            COALESCE(LD.Urban_or_Rural_Area, 'All Areas') AS Area,
            COUNT(AVF.Accident_Index) AS Total_Accidents,
            AVG(AVF.Number_of_Casualties) AS Avg_Casualties
        FROM 
            AccidentVehicleFact AVF
        JOIN 
            DateDimension DD ON AVF.Date_ID = DD.Date_ID
        JOIN 
            LocationDimension LD ON AVF.Location_ID = LD.Location_ID
        GROUP BY 
            ROLLUP (DD.Year, LD.Urban_or_Rural_Area)
        ORDER BY 
            DD.Year, LD.Urban_or_Rural_Area;
    """),
    ("2. Accident Counts by Vehicle Type in Rural Areas", """
        SELECT 
            DD.Year,
            VD.Vehicle_Type,
            COUNT(AVF.Accident_Index) AS Total_Accidents
        FROM 
            AccidentVehicleFact AVF
        JOIN 
            VehicleDimension VD ON AVF.Vehicle_ID = VD.Vehicle_ID
        JOIN 
            DateDimension DD ON AVF.Date_ID = DD.Date_ID
        JOIN 
            LocationDimension LD ON AVF.Location_ID = LD.Location_ID
        WHERE 
            VD.Vehicle_Type IN ('Car', 'Truck') AND LD.Urban_or_Rural_Area = 'Rural'
        GROUP BY 
            DD.Year, VD.Vehicle_Type
        ORDER BY 
            DD.Year, VD.Vehicle_Type;
    """),
    ("3. Weekly Accident Trends", """
        SELECT 
            Year,
            SUM(CASE WHEN Day_of_Week = 'Monday' THEN 1 ELSE 0 END) AS Monday,
            SUM(CASE WHEN Day_of_Week = 'Tuesday' THEN 1 ELSE 0 END) AS Tuesday,
            SUM(CASE WHEN Day_of_Week = 'Wednesday' THEN 1 ELSE 0 END) AS Wednesday,
            SUM(CASE WHEN Day_of_Week = 'Thursday' THEN 1 ELSE 0 END) AS Thursday,
            SUM(CASE WHEN Day_of_Week = 'Friday' THEN 1 ELSE 0 END) AS Friday,
            SUM(CASE WHEN Day_of_Week = 'Saturday' THEN 1 ELSE 0 END) AS Saturday,
            SUM(CASE WHEN Day_of_Week = 'Sunday' THEN 1 ELSE 0 END) AS Sunday
        FROM 
            (SELECT 
                DD.Year,
                DD.Day_of_Week,
                AVF.Accident_Index
            FROM 
                AccidentVehicleFact AVF
            JOIN 
                DateDimension DD ON AVF.Date_ID = DD.Date_ID) AS SubQuery
        GROUP BY 
            Year
        ORDER BY 
            Year;
    """),
    ("4. Accidents by Driver Age and Vehicle Type", """
        SELECT 
            DD2.Age_Band_of_Driver,
            VD.Vehicle_Type,
            COUNT(AVF.Accident_Index) AS Total_Accidents,
            AVG(AVF.Number_of_Casualties) AS Avg_Casualties
        FROM 
            AccidentVehicleFact AVF
        JOIN 
            DriversDimension DD2 ON AVF.Driver_ID = DD2.Driver_ID
        JOIN 
            VehicleDimension VD ON AVF.Vehicle_ID = VD.Vehicle_ID
        GROUP BY 
            DD2.Age_Band_of_Driver, VD.Vehicle_Type
        ORDER BY 
            Total_Accidents DESC;
    """),
    ("5. Top Districts by Casualties", """
        SELECT TOP 10
            COALESCE(LD.Local_Authority_District, 'Total') AS District,
            COALESCE(LD.Urban_or_Rural_Area, 'All Areas') AS Area_Type,
            SUM(AVF.Number_of_Casualties) AS Total_Casualties
        FROM 
            AccidentVehicleFact AVF
        JOIN 
            LocationDimension LD ON AVF.Location_ID = LD.Location_ID
        GROUP BY 
            ROLLUP (LD.Local_Authority_District, LD.Urban_or_Rural_Area)
        ORDER BY 
            Total_Casualties DESC;
    """),
    ("6. High Accident Dates by Year", """
        SELECT TOP 10
            DD.Year,
            COALESCE(CONVERT(VARCHAR, DD.Date), 'Year Total') AS Date,
            COUNT(AVF.Accident_Index) AS Total_Accidents
        FROM 
            AccidentVehicleFact AVF
        JOIN 
            DateDimension DD ON AVF.Date_ID = DD.Date_ID
        GROUP BY 
            CUBE (DD.Year, DD.Date)
        ORDER BY 
            Total_Accidents DESC;
    """),
    ("7. Accidents by Weather and Road Conditions", """
        SELECT TOP 10
            RCD.Weather_Conditions,
            RCD.Road_Surface_Conditions,
            COUNT(AVF.Accident_Index) AS Total_Accidents
        FROM 
            AccidentVehicleFact AVF
        JOIN 
            RoadConditionDimension RCD ON AVF.Location_ID = RCD.RoadCondition_ID
        GROUP BY 
            RCD.Weather_Conditions, RCD.Road_Surface_Conditions
        ORDER BY 
            Total_Accidents DESC;
    """)
]

# Execute and write each query to a file
outFile = 'OLAP_output.txt'  # Specify the output file name
for title, query in queries:
    queryResult(title, query, outFile)
