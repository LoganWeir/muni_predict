from datetime import datetime
import pandas as pd
from geopy import distance
import numpy as np

import random

import pymongo
from pymongo import MongoClient


class ChunkBuilder(object):
    """
    Chunks the data according to the intervals given, finding the most common
    stop at each interval. For each interval, create document with information
    about that interval and its chunks, and add it to the database.
    """

    def __init__(self, trip_collection, chunk_collection, chunk_list):

        self.trip_coll = trip_collection
        self.chunk_coll = chunk_collection
        self.chunk_list = chunk_list

        self.sched = pd.read_csv('data/scheduled_stop_info.csv')

        # Lets sample 20% of the trips for determining chunk stops
        all_trip_ids = self.trip_coll.distinct('trip_id_iso')
        trip_count = len(all_trip_ids)
        sample_size = round(trip_count*.2)
        self.trip_sample = random.sample(all_trip_ids, sample_size)

        # Get the average trip duration from our sample
        self.get_average_duration()


    def get_chunk_info(self):

        # For each chunk interval count
        for chunk_count in self.chunk_list:

            print ("\n")
            print ("Chunking data into ", chunk_count, " chunks")

            chunk_summary = {}
            chunk_summary['number_chunks'] = chunk_count
            chunk_summary['chunks'] = {}

            # Divide Average Trip duration by the number of chunks
            chunk_block = self.avg_duration/chunk_count

            # For each chunk
            for chunk in range(chunk_count):

                # Create a dictionary for each chunk interval
                chunk_seq = chunk+1
                chunk_summary['chunks'][str(chunk_seq)] = {}
                chunk_dict = chunk_summary['chunks'][str(chunk_seq)]

                # If this is the last chunk, use the last schedule stop as the
                # chunk stop
                if chunk_seq == chunk_count:
                    cnk_stp = self.sched.iloc[-1]

                else:
                    # How many seconds into the trip does this chunk occur?
                    time_forward = chunk_block*chunk_seq

                    # Get the location of each sample trip after this time
                    loc_at_chunk = self.locations_at_timestamp(time_forward)

                    # For each stop in our schedule, get the average distance
                    # of each trip, at the chunk interval, to the stop
                    self.sched['avg_chnk_dist'] = self.sched.apply(\
                        lambda row: self.get_avg_dist(row, loc_at_chunk), axis=1)

                    cnk_stp = self.sched.iloc[self.sched['avg_chnk_dist'].idxmin()]

                chunk_dict['chunk_stop_id'] = int(cnk_stp['stop_id'])
                chunk_dict['chunk_sum_dist'] = int(cnk_stp['stop_distance'])
                chunk_dict['chunk_stop_seq'] = int(cnk_stp['stop_sequence'])
                chunk_dict['chunk_stop_lat'] = float(cnk_stp['stop_lat'])
                chunk_dict['chunk_stop_lon'] = float(cnk_stp['stop_lon'])
                chunk_dict['chunk_stop_name'] = cnk_stp['stop_name']

                # Get the distance of the individual chunk by comparing its
                # total distance with that of its prior chunk
                if chunk_seq == 1:
                    chunk_dict['chunk_dist'] = chunk_dict['chunk_sum_dist']
                else:
                    prior_chunk = chunk_summary['chunks'][str(chunk)]
                    pr_chnk_dist = prior_chunk['chunk_sum_dist']
                    crnt_chnk_dist = chunk_dict['chunk_sum_dist']
                    chunk_dict['chunk_dist'] = crnt_chnk_dist - pr_chnk_dist


            self.chunk_coll.insert_one(chunk_summary)


###########
# Utilities

    def get_average_duration(self):
        """
        Gets the mean of all trip durations in our sample
        """

        durations = []

        for trip_id in self.trip_sample:

            start = self.trip_coll.find_one({
                'trip_start': 1,
                'trip_id_iso' : trip_id
            })

            end = self.trip_coll.find_one({
                'trip_end': 1,
                'trip_id_iso' : trip_id
            })

            durations.append(end['time_stamp'] - start['time_stamp'])

        numps = np.array(durations)

        self.avg_duration =  round(numps.mean())


    def locations_at_timestamp(self, chunk_time):
        """
        For each trip in our sample trips, find the document just after the
        given chunk interval, and get its location.
        """

        locations = []

        for samp_id in self.trip_sample:

            # Get the start of the trip
            trip_start = self.trip_coll.find_one({
                "trip_id_iso": samp_id,
                "trip_start": 1
                })

            start_ts = trip_start['time_stamp']

            # Add our chunk interval to the trip start
            frst_tm_chnk = start_ts + chunk_time

            search = {
                'trip_id_iso': samp_id,
                'time_stamp': {"$gt": frst_tm_chnk}
            }

            # Get the first document in our trip after the chunk interval
            chnk_end = list(self.trip_coll.find(search).sort("time_stamp").limit(1))

            # If it exists, get the document's location
            if chnk_end:
                locations.append(\
                    (chnk_end[0]['LATITUDE'], chnk_end[0]['LONGITUDE']))

        return locations


    def get_avg_dist(self, row, location_list):
        """
        Get the average distance from a list of coordinates to a bus stop
        Input:
            - row: a row from our stop schedule, containing the coordinates of a stop
            - location_list: a list of different trip locations at the same time point
        """

        dists = []

        stop_tup = (float(row['stop_lat']), float(row['stop_lon']))

        for loc in location_list:

            dists.append(distance.distance(stop_tup, loc).m)

        return np.array(dists).mean()
