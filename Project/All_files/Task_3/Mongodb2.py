import pymongo
from tabulate import tabulate

# Connect to MongoDB
client = pymongo.MongoClient("mongodb://localhost:27017/")

# Function to execute MongoDB aggregation pipeline and format results
def execute_pipeline(dbName, collectionName, pipeline):
    # Specify the database and collection
    db = client[dbName]
    collection = db[collectionName]

    # Execute the aggregation pipeline
    aggregation_result = collection.aggregate(pipeline)

    # Convert aggregation result to list of dictionaries
    result_list = list(aggregation_result)

    return result_list

# Define the database and collection
db = 'Vehicles_Information'
collection = 'vehicles'

# Define the aggregation pipeline
pipeline = [
    {"$match": {"Year": {"$gte": 2005, "$lte": 2016}}},
    {"$group": {"_id": {"Vehicle_Type": "$Vehicle_Type", "Propulsion_Code": "$Propulsion_Code"}, "count": {"$sum": 1}}},
    {"$project": {"_id": 0, "Vehicle_Type": "$_id.Vehicle_Type", "Propulsion_Code": "$_id.Propulsion_Code", "count": 1}}
]

# Title for the query result
title = "\nAggregating the vehicle type, propulsion code, and their count without the additional grouping by propulsion code"

# Pipeline code for the aggregation
pipeline_code = '''
db.vehicles.aggregate([
  {
    $match: {
      "Year": { $gte: 2005, $lte: 2016 }
    }
  },
  {
    $group: {
      _id: { Vehicle_Type: "$Vehicle_Type", Propulsion_Code: "$Propulsion_Code" },
      count: { $sum: 1 }
    }
  },
  {
    $project: {
      _id: 0,
      Vehicle_Type: "$_id.Vehicle_Type",
      Propulsion_Code: "$_id.Propulsion_Code",
      count: 1
    }
  }
])
'''

# Execute the aggregation pipeline and print result with title and pipeline code
print(title)
print("\nPipeline Code:")
print(pipeline_code)
print("\nResult:")
result = execute_pipeline(db, collection, pipeline)

# Sum up the counts for each vehicle type
vehicle_counts = {}
for entry in result:
    vehicle_type = entry["Vehicle_Type"]
    count = entry["count"]
    if vehicle_type in vehicle_counts:
        vehicle_counts[vehicle_type] += count
    else:
        vehicle_counts[vehicle_type] = count

# Convert vehicle counts to list of dictionaries and add index starting from 1
formatted_result = [{"Index": idx + 1, "Vehicle Type": vehicle_type, "Count of vehicles": count} for idx, (vehicle_type, count) in enumerate(vehicle_counts.items())]

# Print result in tabular format
print(tabulate(formatted_result, headers='keys', tablefmt='grid'))
