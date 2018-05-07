import json

import pymongo
from pymongo import MongoClient

import src.sample_schedule as smpl_schd
import src.build_chunks as bld_chnks
import src.chunk_trips as chnk_trps
import src.trip_chunk_collections as trp_chnks_coll

# Load in our parameters file
with open('parameters.json') as f:
    params = json.load(f)

database = params['database']
labeled_collection = params['labeled_collection']
chunk_collection = params['chunk_collection']
duration_collection = params['duration_collection']
gtfs_period = params['gtfs_period']
chunks = params['chunks']

# Connect to the database
client = MongoClient('localhost', 27017)

# Select our database and collections
db = client[database]
label_coll = db[labeled_collection]
chunk_coll = db[chunk_collection]
duration_coll = db[duration_collection]

# # Start with empty collections
# chunk_coll.delete_many({});
#
# # Create the sample schedule with distances
# smpl_schd.create_sample_schedule(gtfs_period, label_coll)
#
# # Get details, such as the average stop, for various intervals of the data
# chunky = bld_chnks.ChunkBuilder(label_coll, chunk_coll, chunks)
# chunky.get_chunk_info()
#
# # For each trip, label which documents belong to which chunks
# # This will usually take a while, maybe 45 minutes when processing 50 days
# print ("\n")
# print ("Labelling trip documents with different chunks")
# trip_chunker = chnk_trps.TripChunker(label_coll, chunk_coll)
# trip_chunker.chunk_trips()

# Finally, build new collections based on the chunk data from each trip
all_trips = label_coll.distinct('trip_id_iso')

# Total Trip Duration, Time of Day
duration_coll.delete_many({});
trp_chnks_coll.temporal_features_total(all_trips, label_coll, duration_coll)
