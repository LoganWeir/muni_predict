from datetime import datetime
import pandas as pd
from geopy.distance import distance
import numpy as np

import random

import pymongo
from pymongo import MongoClient


class TripChunker(object):
    """
    Labels trip documents accords to which chunks they fall in, based on the
    calucalted chunks in the chunks collection. Chunk Chunk Chunkity Chunk.
    """

    def __init__(self, trip_collection, chunk_collection):

        self.trip_coll = trip_collection
        self.chunk_coll = chunk_collection

        self.all_trip_ids = self.trip_coll.distinct('trip_id_iso')

    def chunk_trips(self):

        # For each trip...
        for idx, trip in enumerate(self.all_trip_ids):

            print ("Chunking Trip ", trip)
            print ("Number ", idx+1, " of ", len(self.all_trip_ids))

            # For each chunk interval set...
            for chunk in self.chunk_coll.find():

                chnk_num = "chunk_" + str(chunk['number_chunks'])

                start_ts = 0

                # For each chunk in that interval...
                for seq, chunk_info in chunk['chunks'].items():

                    # For accuring the closest document
                    best_dist = {
                        'stop_dist':100000,
                        'time_stamp': 0}

                    filter_search = {
                        "trip_id_iso": trip,
                        "time_stamp": {"$gte": start_ts}
                    }

                    # For each document in the trip...
                    # Yes, a quadruple inset for-loop!
                    # Test each trip document to see which is closest to the
                    # chunk stop
                    for doc in self.trip_coll.find(filter_search).sort('time_stamp'):

                        doc_lat = doc['LATITUDE']
                        doc_lon = doc['LONGITUDE']

                        cnk_lat = chunk_info['chunk_stop_lat']
                        cnk_lon = chunk_info['chunk_stop_lon']

                        doc_dist = distance((doc_lat, doc_lon), (cnk_lat, cnk_lon)).m

                        # Update the accural dictionary if a better distance is
                        # found
                        if doc_dist < best_dist['stop_dist']:
                            best_dist['stop_dist'] = doc_dist
                            best_dist['time_stamp'] = doc['time_stamp']

                    # Given our closest time, search all documents in this trip
                    # that occur after our last chunk and before the closest time
                    label_search = {
                        "trip_id_iso": trip,
                        "time_stamp": {
                            "$gte": start_ts,
                            "$lt": best_dist['time_stamp']
                        }
                    }

                    for doc in self.trip_coll.find(label_search).sort('time_stamp'):

                        doc_id = doc['_id']

                        self.trip_coll.update_one({"_id": doc_id}, {
                            "$set": {
                                chnk_num: seq
                            }
                        })


                    start_ts = best_dist['time_stamp']
