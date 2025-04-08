import pymongo
from tabulate import tabulate

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Function to execute MongoDB NoSQL query and format results
def execute_query(dbName, collectionName, query):
    # Specify the database and collection
    db = client[dbName]
    collection = db[collectionName]

    # Execute the query and retrieve the documents
    documents = collection.find(query)

    # Initialize dictionary to store counts for each vehicle type
    vehicle_counts = {}

    # Count the occurrences of each vehicle type
    for doc in documents:
        vehicle_type = doc['Vehicle_Type']
        if vehicle_type in vehicle_counts:
            vehicle_counts[vehicle_type] += 1
        else:
            vehicle_counts[vehicle_type] = 1

    # Prepare data for tabular format
    data = [{'Vehicle Type': vehicle_type, 'Count': count} for vehicle_type, count in vehicle_counts.items()]

    # Format the data in tabular format
    result_table = tabulate(data, headers='keys', tablefmt='grid')

    return result_table

# Define the database and collection
db = 'Vehicles_Information'
collection = 'vehicles'

# Define the NoSQL query
query = {"Year": {"$gte": 2005, "$lte": 2016}, "Vehicle_Type": "Motorcycle over 500cc"}

# Title for the query result
title = "\nA Query to count the number of documents where the Year field is between 2005 and 2016 (inclusive) and the Vehicle_Type field is 'Motorcycle over 500cc'"

# NoSQL code for the query
nosql_code = 'db.vehicles.countDocuments({"Year": { $gte: 2005, $lte: 2016 }, "Vehicle_Type": "Motorcycle over 500cc"})'

# Execute the NoSQL query and print result with title and NoSQL code
print(title)
print("\nNoSQL Code:", nosql_code)
print("\nResult:")
print(execute_query(db, collection, query))

