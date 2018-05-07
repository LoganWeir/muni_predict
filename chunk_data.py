import json

import pymongo
from pymongo import MongoClient

import src.sample_schedule as smpl_schd
import src.build_chunks as bld_chnks
import src.chunk_trips as chnk_trps

# Load in our parameters file
with open('parameters.json') as f:
    params = json.load(f)

database = params['database']
labeled_collection = params['labeled_collection']
chunk_collection = params['chunk_collection']
gtfs_period = params['gtfs_period']
chunks = params['chunks']

# Connect to the database
client = MongoClient('localhost', 27017)

# Select our database and collections
db = client[database]
label_coll = db[labeled_collection]
chunk_coll = db[chunk_collection]

# Start with empty collections
chunk_coll.delete_many({});

# Create the sample schedule with distances
smpl_schd.create_sample_schedule(gtfs_period, label_coll)

# Get details, such as the average stop, for various intervals of the data
chunky = bld_chnks.ChunkBuilder(label_coll, chunk_coll, chunks)
chunky.get_chunk_info()

# For each trip, label which documents belong to which chunks
print ("\n")
print ("Labelling trip documents with different chunks")
trip_chunker = chnk_trps.TripChunker(label_coll, chunk_coll)
trip_chunker.chunk_trips()
