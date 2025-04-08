import pymongo
from tabulate import tabulate

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Function to execute MongoDB count query and return the count
def execute_count_query(dbName, collectionName, query):
    # Specify the database and collection
    db = client[dbName]
    collection = db[collectionName]

    # Execute the count query
    count = collection.count_documents(query)

    return count
# Define the database and collection
db = 'Vehicles_Information'
collection = 'vehicles'

# Define the count query
count_query = {"Propulsion_Code": "Petrol", "Year": {"$gte": 2005, "$lte": 2016}}

# Title for the query result
title = "Query to count the number of vehicles using petrol from the years 2005 to 2016"

# NoSQL code for the count query
nosql_code = 'db.vehicles.count({"Propulsion_Code": "Petrol", "Year": { $gte: 2005, $lte: 2016 }})'

# Extract Propulsion Code from NoSQL code
propulsion_code = nosql_code.split('"')[3]

# Execute the count query and print result with title and NoSQL code
print(title)
print("NoSQL Code:")
print(nosql_code)
print("\nResult:")
result = execute_count_query(db, collection, count_query)

# Format result for tabular display
formatted_result = [{"Propulsion Code": propulsion_code, "Vehicle Count": result}]

# Print result in tabular format
print(tabulate(formatted_result, headers='keys', tablefmt='grid'))
