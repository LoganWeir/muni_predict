import pandas as pd
from datetime import datetime
import time
import numpy as np
import pymongo
from pymongo import MongoClient
from geopy import distance
import random
import string

class TripLabeling(object):
    """
    Class for labeling raw AVL data with trip_ids
    Data should all be:
        From one distinct GTFS period
        For one route
        In one direction
    """

    def __init__(self, raw_collection, start_collection,
                    trip_collection, gtfs_period=0):
        """
        Input:
            in_collection: Collection we are pulling raw data from
            out_collection: Collection we are inserting labeled data into
        """

        self.raw_coll = raw_collection
        self.strt_coll = start_collection
        self.trip_coll = trip_collection

        # Get all unique blocks in the gtfs-specific collection
        self.blocks = self.raw_coll.distinct('TRAIN_ASSIGNMENT')

        # Turn these blocks to integers for later lookup
        self.int_blocks = [int(blk) for blk in self.blocks]

        self.get_gtfs_dir(gtfs_period)
        self.load_filter_gtfs()

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


    #########
    # Trip labelling with the starts
    def label_trips(self):
        """
        Using labeled starts to label entire trips, by iterating through
        subsequent data until an intersection with the end stop is detected
        """

        self.testdocs = {}

        self.good_doc_count = 0

        self.mini = []
        self.giant = []
        self.endless = []
        self.empty = 0

        # For each start in our out collection
        for start in self.strt_coll.find({ 'trip_start': 1}):

                # Build our search parameters for the in collection
                search = {}
                search['TRAIN_ASSIGNMENT'] = start['TRAIN_ASSIGNMENT']
                search['VEHICLE_TAG'] = str_vehicle = start['VEHICLE_TAG']
                search['time_stamp'] = {"$gt": start['time_stamp']}

                # Get the lat/lon of the last stop of this trip
                last_stop = self.get_last_stop(start)

                # Get the tripid_iso identifier with which to label the documents
                tripid_iso = start['trip_id_iso']

                # Get a list of all labeled documents on this trip
                trip_docs = self.get_trip_docs(search, last_stop, tripid_iso)

                # If trip_docs isn't None, all it to the clean dictionary
                if trip_docs:
                    self.testdocs[tripid_iso] = trip_docs
                    self.good_doc_count += len(trip_docs)

        # Print labelling stats
        print ("Total Starts: ", self.strt_coll.count())
        print ("\n")
        print ("Total Good Trips: ", len(self.testdocs))
        print ("\n")
        print ("Total Emtpy Trips: ", self.empty)
        print ("\n")
        print ("Total Sparse Trips: ", len(self.mini))
        print ("\n")
        print ("Total Dense Trips: ", len(self.giant))
        print ("\n")
        print ("Total 'Endless' Trips: ", len(self.endless))
        totes_count = len(self.testdocs) + self.empty + len(self.mini) \
            + len(self.giant) + len(self.endless)
        print ("Total Trips: ", totes_count)
        print ("\n")
        print ("Total Good, Labeled Trip Docs: ", self.strt_coll.count() \
            + self.good_doc_count)

        # Add all clean trips to the output collection
        for key, value in self.testdocs.items():
            self.add_to_out_collection(value)

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

    def get_trip_docs(self, search_params, last_stop, tripid_iso):
        """
        Gather and label all documents that follow a start, up until an
        intersection with the last stop is detected.
        Input:
            search_params: dictionary of search params based on the labeled start
            last_stop: last stop of the labeled start's trip
            tripid_iso: Unique label to apply to all documents within the trip
        Output: List of documents labeled with the trip_id
        """

        # Check if there are enough/too many docs in the trip
        count = 0

        # Check it our trip every 'ended'
        breakin = 0

        # Where to collect our labeled trip_docs
        trip_docs = []

        # Get all relevant docs after our search, sorted
        search = self.raw_coll.find(search_params).sort('time_stamp')

        # Account for starts that occur right at the end of our data
        if search.count() == 0:
            self.empty += 1
            return None

        # Get all documents that match our search, sorted by time_stamp
        for data in search:

            # Add the label to the document
            data['trip_id_iso'] = tripid_iso

            # Get the document's lat/lon
            data_latlon = (data['LATITUDE'], data['LONGITUDE'])

            # Check for last_stop intersection
            if distance.distance(last_stop, data_latlon).m <= 150:

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
            self.endless.append(trip_docs)

            return None

        # Check if the trip is unreasonably sparse
        elif count < 40:

            # Add the trip to a separate array
            self.mini.append(trip_docs)

            return None

        # Check if trip is unreasonably dense
        elif count > 150:

            # Add the trip to a separate array
            self.giant.append(trip_docs)

            return None

        # Otherwise, return our wonderful, clean trip!
        else:

            return trip_docs

    def add_to_out_collection(self, list):
        """
        Takes a list of dictionaries and adds them to the output dictionary
        """
        for doc in list:
            # Upsert, in case the document already exists in the DB
            self.trip_coll.update_one({'_id':doc['_id']}, {'$set':doc}, upsert=True)