import json

import pymongo
from pymongo import MongoClient

import src.extract as extract
import src.label_starts as label_starts
import src.label_trips as label_trips


# Load in our parameters file
with open('parameters.json') as f:
    params = json.load(f)

# Get relevant parameters
database = params['database']
avl_collection = params['avl_collection']
labeled_collection = params['labeled_collection']
gtfs_period = params['gtfs_period']
days = params['ftp_days']
bus = params['bus']
direction = params['direction']

# Connect to the database
client = MongoClient('localhost', 27017)

# Select our database and collections
db = client[database]
raw_coll = db[avl_collection]
label_coll = db[labeled_collection]

# Start with empty collections
raw_coll.delete_many({});
label_coll.delete_many({});

# Extract the data from the FTP Server
extractor = extract.Extractor(raw_coll, gtfs_period=gtfs_period, days=days,
                                bus=bus, direction=direction)
extractor.run()

# Label Trip Starts in the Data
start_labeler = label_starts.StartLabeler(raw_coll, label_coll,
                        gtfs_period=gtfs_period)
start_labeler.label_single_starts()

# Label the remaining data based on the starts
trip_labeler = label_trips.TripLabeler(raw_coll, label_coll,
                        gtfs_period=gtfs_period)
trip_labeler.label_trips()
