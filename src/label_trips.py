import pandas as pd
from datetime import datetime
import time
import numpy as np
import pymongo
from pymongo import MongoClient
from geopy import distance
import random
import string

class TripLabeler(object):
    """
    Class for labeling raw AVL data with trip_ids
    Data should all be:
        From one distinct GTFS period
        For one route
        In one direction
    """

    def __init__(self, raw_collection, trip_collection, gtfs_period=0):
        """
        Input:
            raw_collection:
                Collection we are pulling raw data from
            trip_collection:
                Collection we are inserting labeled data into
            gtfs_period:
                Index of the gtfs period we wish to get data for.
                Indices can be looked up in data/gtfs_lookup.csv. The file is
                sorted with most recent periods first
        """

        self.raw_coll = raw_collection
        self.trip_coll = trip_collection

        # Get all unique blocks in the gtfs-specific collection
        self.blocks = self.raw_coll.distinct('TRAIN_ASSIGNMENT')

        # Turn these blocks to integers for later lookup
        self.int_blocks = [int(blk) for blk in self.blocks]

        # Setup the instance
        self.get_gtfs_dir(gtfs_period)
        self.load_filter_gtfs()
        self.get_max_last_stop()

    #########
    # Pipeline Setup

    def get_gtfs_dir(self, gtfs_period):
        """
        Sets the date range of the extractor and gtfs directory
        Input:
            gtfs_period: Index of the gtfs_lookup file to use
        """

        gtfs_df = pd.read_csv('data/gtfs_lookup.csv')
        gtfs_series = gtfs_df.iloc[gtfs_period]

        self.gtfs_directory = gtfs_series['directory']

    def load_filter_gtfs(self):
        """
        Loads in trips, schedules, stops and the calendar from the local GTFS
        directory, filtering them based on the blocks.
        Requires self.gtfs_directory
        """

        # Trips
        trip_txt = 'data/gtfs/{}/trips.txt'.format(self.gtfs_directory)
        trips = pd.read_csv(trip_txt)
        self.trip_blocks = trips[(trips['block_id'].isin(self.int_blocks)) \
            & (trips['direction_id'] == 0)]
        trip_ids = self.trip_blocks['trip_id'].unique()

        # Schedule
        sched_txt = 'data/gtfs/{}/stop_times.txt'.format(self.gtfs_directory)
        sched = pd.read_csv(sched_txt)
        self.sched_trps = sched[sched['trip_id'].isin(trip_ids)]
        stop_ids = self.sched_trps['stop_id'].unique()

        # Stops
        stop_txt = 'data/gtfs/{}/stops.txt'.format(self.gtfs_directory)
        stops = pd.read_csv(stop_txt)
        self.stop_sched = stops[stops['stop_id'].isin(stop_ids)]

        # Calendar
        cal_txt = 'data/gtfs/{}/calendar.txt'.format(self.gtfs_directory)
        calendar = pd.read_csv(cal_txt)

        # Since the datetime module gives day the week as an interger (0 for Monday,
        # 6 for Sunday), we need to relabel the calendar columns
        cal_col_mapping = {'monday':0, 'tuesday':1, 'wednesday':2, 'thursday':3,
                       'friday':4, 'saturday':5, 'sunday':6}
        self.cal_colnum = calendar.rename(columns=cal_col_mapping)

    def get_max_last_stop(self):
        """
        We want to get the last stop of the longest possible trip of this route.
        This way, we will build models across similar trips, but can still make
        predictions on smaller trips that are subsections of the biggest trip.
        """

        # Get the id of the last stop in the longest trip
        seq_max_idx = self.sched_trps['stop_sequence'].idxmax()
        max_stop_id = self.sched_trps.loc[seq_max_idx]['stop_id']

        # Get the stop, and pull out it's latitude and longitude
        ed_stp = self.stop_sched[self.stop_sched['stop_id'] == max_stop_id]
        edstp_ltln = (ed_stp['stop_lat'].values[0], ed_stp['stop_lon'].values[0])

        self.last_stop = edstp_ltln


    #########
    # Trip labelling with the starts
    def label_trips(self):
        """
        Using labeled starts to label entire trips, by iterating through
        subsequent data until an intersection with the end stop is detected
        """

        # Count up good/bap trips
        self.good_trip_count = 0
        self.good_doc_count = 0
        self.mini = 0
        self.giant = 0
        self.endless = 0
        self.empty = 0
        self.sparse = 0

        # For each start in our out collection
        for start in self.trip_coll.find({ 'trip_start': 1}):

            # Build our search parameters for the in collection
            search = {}
            search['TRAIN_ASSIGNMENT'] = start['TRAIN_ASSIGNMENT']
            search['VEHICLE_TAG'] = start['VEHICLE_TAG']

            # Get documents that occur after the trip start, up to 3 hours
            plus_3hr = start['time_stamp'] + 10800
            search['time_stamp'] = {"$gt": start['time_stamp'], "$lt": plus_3hr}

            # Don't get other trip starts
            search['trip_start'] = { "$exists": False}

            # Don't get previously labeled data
            search['trip_id_iso'] = {"$exists": False}

            # # Get the lat/lon of the last stop of this trip
            # last_stop = self.get_last_stop(start)

            # Get the tripid_iso identifier with which to label the documents
            tripid_iso = start['trip_id_iso']

            # Get a list of all labeled documents on this trip
            self.get_trip_docs(search, tripid_iso)


        start_count = self.trip_coll.find({ 'trip_start': 1}).count()

        # Print labelling stats
        print ("----------------")
        print ("Total Good Trips: ", self.good_trip_count)
        print ("\n")
        print ("Total Good Documents", self.good_doc_count + start_count)
        print ("\n")
        print ("Total Emtpy Trips: ", self.empty)
        print ("\n")
        print ("Total Mini Trips: ", self.mini)
        print ("\n")
        print ("Total Gigantic Trips: ", self.giant)
        print ("\n")
        print ("Total 'Endless' Trips: ", self.endless)
        print ("\n")
        print ("Total Sparse Trips: ", self.sparse)
        print ("\n")


    def get_last_stop(self, start):
        """
        Gets the last stop for a labeled start's trip
        Input: A labeled start document
        Output: A tuple with the lat/lon of the trips last stop
        """

        # Get the id of the last stop
        trip_id = start['trip_id']
        trip_sched = self.sched_trps[self.sched_trps['trip_id'] == trip_id]
        lst_stop_id = trip_sched.tail(1)['stop_id'].values[0]

        # Get the stop, and pull out it's latitude and longitude
        ed_stp = self.stop_sched[self.stop_sched['stop_id'] == lst_stop_id]
        edstp_ltln = (ed_stp['stop_lat'].values[0], ed_stp['stop_lon'].values[0])

        return edstp_ltln


    def get_trip_docs(self, search_params, tripid_iso):
        """
        Gather and label all documents that follow a start, up until an
        intersection with the last stop is detected.
        Input:
            search_params: dictionary of search params based on the labeled start
            tripid_iso: Unique label to apply to all documents within the trip
        Output: List of documents labeled with the trip_id
        """

        # Check if there are enough/too many docs in the trip
        count = 0

        # Check if the trip is too sparsely sampled
        sparse = False

        # Check it our trip every 'ended'
        breakin = 0

        # Where to collect our labeled trip_docs
        trip_docs = []

        # Get all relevant docs after our search, sorted
        search = list(self.raw_coll.find(search_params).sort('time_stamp'))

        # Account for starts that occur right at the end of our data
        if not search:
            self.empty += 1
            self.trip_coll.delete_one({'trip_id_iso': tripid_iso})
            return None

        # Get all documents that match our search, sorted by time_stamp
        for idx, data in enumerate(search):
            data_ts = data['time_stamp']

            if idx != 0:
                last_doc = search[idx-1]
                last_ts = last_doc['time_stamp']
                diff = data_ts - last_ts

                # If the time_stamp of this document is 5 minutes after the next:
                if diff > 180:
                    sparse = True
                    breakin += 1
                    break

            # Add the label to the document
            data['trip_id_iso'] = tripid_iso

            # Get the document's lat/lon
            data_latlon = (data['LATITUDE'], data['LONGITUDE'])

            # Check for last_stop intersection
            if distance.distance(self.last_stop, data_latlon).m <= 150:

                # Label the document as the end
                data['trip_end'] = int(1)

                # Add the document to our output list
                trip_docs.append(data)

                # Make sure to record each doc, even the last!
                count += 1

                # Acknowledge that we broke the loop
                breakin += 1

                # Break the loop
                break


            # Add to count
            count += 1

            # Add the document to our output list
            trip_docs.append(data)


        # Check for lack of ending intersection! :-(
        if breakin == 0:

            # Add the trip to a separate array
            self.endless += 1
            self.trip_coll.delete_one({'trip_id_iso': tripid_iso})
            return None

        elif sparse == True:

            self.sparse += 1
            self.trip_coll.delete_one({'trip_id_iso': tripid_iso})
            return None

        # Check if the trip is unreasonably sparse
        elif count < 40:

            # Add the trip to a separate array
            self.mini += 1
            self.trip_coll.delete_one({'trip_id_iso': tripid_iso})
            return None

        # Check if trip is unreasonably dense
        elif count > 150:

            # Add the trip to a separate array
            self.giant +=1
            self.trip_coll.delete_one({'trip_id_iso': tripid_iso})
            return None

        # Otherwise, return our wonderful, clean trip!
        else:
            self.good_trip_count += 1
            self.good_doc_count += len(trip_docs)
            self.add_to_out_collection(trip_docs)
            return None


    def add_to_out_collection(self, list):
        """
        Takes a list of dictionaries and adds them to the output dictionary
        """
        for doc in list:
            # Upsert, in case the document already exists in the DB
            self.trip_coll.update_one({'_id':doc['_id']}, {'$set':doc}, upsert=True)
