import pymongo
from tabulate import tabulate
import pandas as pd

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Function to execute MongoDB aggregation query and format results
def aggregate_query(dbName, collectionName, pipeline, title):
    # Specify the database and collection
    db = client[dbName]
    collection = db[collectionName]

    # Execute the aggregation pipeline
    pipe_output = collection.aggregate(pipeline)

    # Initialize the result string with title
    result_with_title = "\n"+ title + "\n"

    # Add the pipeline code to the result string
    result_with_title += "Pipeline code:\n"
    result_with_title += str(pipeline) + "\n\n"

    # Iterate over each year's data
    for document in pipe_output:
        year = document["_id"]
        vehicles = document["vehicles"]

        # Sum up the counts of all vehicle types for the year
        total_count = sum(vehicle["count"] for vehicle in vehicles)

        # Create DataFrame for the year's data
        df = pd.DataFrame(vehicles)

        # Format DataFrame as table
        result_table = tabulate(df, headers="keys", tablefmt="grid", showindex=False)

        # Add year and counts to the result string
        result_with_title += f"\nYear: {year}\n{result_table}\nTotal count of vehicles: {total_count}\n\n"

    return result_with_title

# Define the database and collection
db = 'Vehicles_Information'
collection = 'vehicles'

# Define the MongoDB aggregation pipeline
pipeline = [
    {"$group": {
        "_id": {"Year": "$Year", "Vehicle_Type": "$Vehicle_Type"},
        "total_count": {"$sum": 1}
    }},
    {"$sort": {"_id.Year": 1, "total_count": -1}},
    {"$group": {
        "_id": "$_id.Year",
        "vehicles": {
            "$push": {
                "type": "$_id.Vehicle_Type",
                "count": "$total_count"
            }
        }
    }},
    {"$sort": {"_id": 1}}
]

# Title for the aggregation result
title = "5. The pipeline aggregates, groups, and sorts vehicle data by year and type\n"

# Execute the MongoDB aggregation query and print results with title and pipeline code
result_with_title = aggregate_query(db, collection, pipeline, title)
print(result_with_title)
