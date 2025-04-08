import pandas as pd
from tabulate import tabulate
import pyodbc
import os, sys

# Setup the database connection
connection_string = "Driver={ODBC Driver 17 for SQL Server};Server=DESKTOP-B8D6R50\\SQLEXPRESS;Database=TermProject;Trusted_Connection=yes;"
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
    ("1. Accident Severity Distribution", """
        SELECT Accident_Severity, COUNT(*) AS Total_Accidents
        FROM AccidentCoreDetails
        GROUP BY Accident_Severity;
    """),
    ("2. Impact of Light Conditions on Accidents", """
        SELECT Light_Conditions, COUNT(*) AS Number_of_Accidents, AVG(Number_of_Casualties) AS Average_Casualties
        FROM AccidentSummaryView
        GROUP BY Light_Conditions;
    """),
    ("3. Detailed Accident Reports for Multi-Vehicle Incidents", """
        SELECT TOP 100 a.Date, a.Time, a.Urban_or_Rural_Area, v.Vehicle_Type, d.Age_Band_of_Driver
        FROM AccidentCoreDetails a
        JOIN VehicleDetails v ON a.Accident_Index = v.Accident_Index
        JOIN DriverDetails d ON v.Driver_ID = d.Driver_ID
        WHERE a.Accident_Index IN (SELECT Accident_Index FROM AccidentCoreDetails WHERE Number_of_Vehicles > 1);
    """),
    ("4. Vehicle Type and Driver Age Band Analysis", """
        SELECT Age_Band_of_Driver, Vehicle_Type, COUNT(*) AS Total
        FROM VehicleDriverDetailsView
        WHERE Age_Band_of_Driver != 'Data missing or out of range'
        GROUP BY Age_Band_of_Driver, Vehicle_Type
        ORDER BY Total DESC;
    """),
    ("5. Vehicle Types Involved by Accident Severity", """
        SELECT 
            a.Accident_Severity, 
            v.Vehicle_Type, 
            COUNT(*) AS Total_Vehicles
        FROM 
            AccidentCoreDetails AS a
        JOIN 
            VehicleDetails AS v ON a.Accident_Index = v.Accident_Index
        GROUP BY 
            a.Accident_Severity, 
            v.Vehicle_Type
        ORDER BY 
            a.Accident_Severity, 
            Total_Vehicles DESC;
    """),
    ("6. Urban Accidents in Adverse Conditions", """
        SELECT TOP 100
            a.Accident_Index, 
            a.Date,  
            v.Vehicle_Type, 
            v.Age_of_Vehicle
        FROM 
            AccidentCoreDetails AS a
        JOIN 
            VehicleDetails AS v ON a.Accident_Index = v.Accident_Index
        JOIN 
            LocationAndEnvironmentDetails AS l ON a.Accident_Index = l.Accident_Index
        WHERE 
            a.Urban_or_Rural_Area = 'Urban'
            AND l.Light_Conditions = 'Darkness - lights lit'
            AND l.Weather_Conditions LIKE '%rain%'
            AND v.Age_of_Vehicle > 10;
    """)
]

# Execute and write each query to a file
outfile = 'OLTP_queries.txt'  # Specify the output file name
for title, query in queries:
    queryResult(title, query, outfile)
