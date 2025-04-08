import pymongo
from tabulate import tabulate

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Function to execute MongoDB NoSQL query and format results
def execute_query(dbName, collectionName, query):
    # Specify the database and collection
    db = client[dbName]
    collection = db[collectionName]

    # Execute the query and retrieve the count of documents
    count = collection.count_documents(query)

    return count

# Define the database and collection
db = 'Accidents_information'
collection = 'Accidents'

# Define the NoSQL query
query = {"Urban_or_Rural_Area": "Urban", "Speed_limit": {"$gt": 30}}

# Title for the query result
title = "\nA query that counts the number of documents in the Accidents collection where the 'Urban_or_Rural_Area' field has the value 'Urban' and the 'Speed_limit' field has a value greater than 30."

# NoSQL code for the query
nosql_code = 'db.Accidents.countDocuments({"Urban_or_Rural_Area": "Urban", "Speed_limit": { $gt: 30 }})'

# Format the area and speed conditions
area_condition = f"Urban_or_Rural_Area: Urban"
speed_condition = f"Speed_limit > 30"

# Execute the NoSQL query and print result with title, NoSQL code, area condition, and speed condition
print(title)
print("\nNoSQL Code:", nosql_code)
print("\nResult:")
result_count = execute_query(db, collection, query)
data = [{'Area': 'Urban', 'Speed Limit': '> 30', 'Count of Accidents': result_count}]
print(tabulate(data, headers='keys', tablefmt='grid'))
