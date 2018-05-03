from datetime import datetime
import pandas as pd
from geopy import distance
import numpy as np
import pprint

import pymongo
from pymongo import MongoClient

class Formatter(object):
    """
    Formats labeled rows into a trip
    """
    def __init__(self, input_collection, output_collection, gtfs_period=0):

        self.in_coll = input_collection
        self.out_coll = output_collection

        # Get all unique trips in the input database
        self.trips = self.in_coll.distinct('trip_id_iso')

        # Get all unique trip_ids for filtering out the schedule and stops
        self.trip_ids = self.in_coll.distinct('trip_id')


        self.get_gtfs_dir(gtfs_period)

        # Run all the setup funtions
        self.setup()




        # # Format trips
        # self.format_trips()

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

    def setup(self):
        """
        Runs all the setup functions together.
        """

        self.load_filter_gtfs()
        self.cleanup_trips()

    def load_filter_gtfs(self):
        """
        Loads in trips, schedules, stops and the calendar from the local GTFS
        directory, filtering them based on the blocks.
        Requires self.gtfs_directory
        """

        # Schedule
        sched_txt = 'data/gtfs/{}/stop_times.txt'.format(self.gtfs_directory)
        sched = pd.read_csv(sched_txt)
        self.sched_trps = sched[sched['trip_id'].isin(self.trip_ids)]
        stop_ids = self.sched_trps['stop_id'].unique()

        # Stops
        stop_txt = 'data/gtfs/{}/stops.txt'.format(self.gtfs_directory)
        stops = pd.read_csv(stop_txt)
        self.stop_sched = stops[stops['stop_id'].isin(stop_ids)]

    def cleanup_trips(self):

        clean_trips = []

        for trip in self.trips:

            trip_cursor = self.in_coll.find({'trip_id_iso': trip})

            # Avoid those weird small trips (all start, no end)
            if trip_cursor.count() < 40:
                continue

            trip_id_cursor = self.in_coll.find(
                {'trip_id_iso': trip,
                'trip_id' : {'$ne' : None} })

            if trip_id_cursor.count() == 0:

                print ("NOPPPPPE")

            # # Avoid trips that are abnormally high (NEED TO FIX THIS, BLIND)
            # # I could be missing long trips, as opposed to ingest flukes
            # if trip_cursor.count() < 100:
            #     continue

            clean_trips.append(trip)

        self.trips = clean_trips




    #########
    # Formatting Trips

    def format_trips(self):

        for trip in self.trips:

            trip_cursor = self.in_coll.find({'trip_id_iso': trip}, {'_id': False})

            # Load the trip data into a dataframe
            self.trip_df = pd.DataFrame(list(trip_cursor))
            self.trip_df.sort_values(by=['time_stamp'])

            # Compare min/max timestamps to filter out flukes!
            # This is inefficent, could be merged with get_trip_times
            if self.min_max_time_diff(self.trip_df) > 5400:
                continue

            # Create a dictionary to store the trip details in
            self.trip_dict = {}

            # Get the trip_id and the trip_id_iso
            self.trip_dict['trip_id_iso'] = trip
            self.trip_id = self.trip_df['trip_id'].unique()[0]
            self.trip_dict['trip_id'] = int(self.trip_id)

            self.get_trip_times()

            # Get the schedule of the trip
            schedule_mask = self.sched_trps['trip_id'] == self.trip_id
            self.sched_trp_df = self.sched_trps[schedule_mask]
            self.sched_trp_df = self.sched_trp_df.sort_values(by=['stop_sequence'])
            self.sched_trp_df = self.sched_trp_df.reset_index(drop=True)

            self.max_sequence = str(self.sched_trp_df['stop_sequence'].max())

            # Convert sequencne to string for mongodb keys
            self.sched_trp_df['stop_sequence'] = \
                self.sched_trp_df['stop_sequence'].astype(str)


            # Get the starts and edges by iterating through the schedule
            self.get_stops_edges()

            self.out_coll.update_one({'_id':self.trip_dict['trip_id_iso']},
                                    {'$set':self.trip_dict}, upsert=True)

        # return self.sched_trp_df

    def get_trip_times(self):
        """
        Get the start, end, and duration of the trip
        """

        trip_time_dict = {}

        first_trip_ts = self.trip_df['time_stamp'].iloc[0]
        trip_time_dict['first_timestamp'] = first_trip_ts

        last_trip_ts = self.trip_df['time_stamp'].iloc[-1]
        trip_time_dict['last_timestamp'] = last_trip_ts

        first_dt = datetime.fromtimestamp(first_trip_ts)
        last_dt = datetime.fromtimestamp(last_trip_ts)
        trip_diff = last_dt - first_dt
        trip_time_dict['trip_duration'] = trip_diff.total_seconds()

        self.trip_dict.update(trip_time_dict)

    def get_stops_edges(self):

        self.trip_dict['stops'] = {}
        self.trip_dict['edges'] = {}

        for idx, series in self.sched_trp_df.iterrows():

            # Later if I want to search distance from one point forward
            # self.search_idx = 0

            seq = series['stop_sequence']

            if seq == '1':
                self.get_stop_info(series)

            if seq != self.max_sequence:

                nxt_stp = self.sched_trp_df.iloc[idx + 1]
                self.get_stop_info(nxt_stp)

                seq2 = nxt_stp['stop_sequence']

                self.get_edge(seq, seq2)

        # pprint.pprint(self.trip_dict['edges'])

    def get_stop_info(self, series):

        stop_info = {}

        stop_id = series['stop_id']
        stop_info['stop_id'] = int(stop_id)

        sequence = series['stop_sequence']

        # Save our non-key sequences as actual ints
        stop_info['sequence'] = int(sequence)

        stop_row = self.stop_sched[self.stop_sched['stop_id'] == stop_id]
        stop_latlon = (float(stop_row['stop_lat']), float(stop_row['stop_lon']))
        stop_info['stop_latlon'] = stop_latlon

        # Make new dataframe with a distance column
        dist_df = self.trip_df.copy()

        dist_df['distance'] = dist_df.apply\
            (lambda row: self.get_distance(stop_latlon, row), axis=1)

        close_mask = dist_df['distance'] < 50
        close_df = dist_df[close_mask]


        # If none with 100 meters, get the closest
        if close_df.shape[0] == 0:

            close_idx = dist_df['distance'].idxmin()
            close_df = dist_df.iloc[close_idx]
            # Convert to DF
            close_df = close_df.to_frame().T

        # If only one timestamp
        if close_df['time_stamp'].shape[0] == 1:
            timestamp = (close_df['time_stamp'].values[0])
            first_ts = timestamp
            last_ts = timestamp
            duration = 0

        else:
            min_idx = close_df['time_stamp'].idxmin()
            max_idx = close_df['time_stamp'].idxmax()

            first_ts = close_df.loc[min_idx]['time_stamp']
            last_ts = close_df.loc[max_idx]['time_stamp']

            first_dt = datetime.fromtimestamp(first_ts)
            last_dt = datetime.fromtimestamp(last_ts)

            diff = last_dt - first_dt

            duration = diff.total_seconds()

        stop_info['first_intersect_ts'] = first_ts
        stop_info['last_intersect_ts'] = last_ts
        stop_info['seconds_at_stop'] = duration

        self.trip_dict['stops'][str(sequence)] = stop_info

    def get_edge(self, seq, seq2):

        edge_info = {}
        edge_info['sequence'] = int(seq)

        stop_1 = self.trip_dict['stops'][seq]
        stop_2 = self.trip_dict['stops'][seq2]

        edge_info['from'] = int(stop_1['stop_id'])
        edge_info['to'] = int(stop_2['stop_id'])

        s1_lst_ts = stop_1['last_intersect_ts']
        s2_frst_ts = stop_2['first_intersect_ts']

        edge_info['first_timestamp'] = s1_lst_ts
        edge_info['last_timestamp'] = s2_frst_ts

        first_dt = datetime.fromtimestamp(s1_lst_ts)
        last_dt = datetime.fromtimestamp(s2_frst_ts)
        diff = last_dt - first_dt
        duration = diff.total_seconds()

        if duration < 0:
            edge_info['edge_time'] = 0
        else:
            edge_info['edge_time'] = duration

        self.trip_dict['edges'][seq] = edge_info

    def min_max_time_diff(self, df):

        min_idx = df['time_stamp'].idxmin()
        max_idx = df['time_stamp'].idxmax()

        first_ts = df.loc[min_idx]['time_stamp']
        last_ts = df.loc[max_idx]['time_stamp']

        first_dt = datetime.fromtimestamp(first_ts)
        last_dt = datetime.fromtimestamp(last_ts)

        diff = last_dt - first_dt

        return diff.total_seconds()

    def get_distance(self, stop_tuple, series):
        """
        Input:
            stop_tuple: Tuple of stop lat/lon
            row_tuple: Tuple of row lat/lon
        """

        row_latlon = (float(series['LATITUDE']), float(series['LONGITUDE']))

        return distance.distance(stop_tuple, row_latlon).m



        # print (len(close_df.columns))
        # # print (type(close_df))
        # #

        # # print (max_idx)





        # print (close_df)



        # # Collect the time_stamps of rows that intersected this stop
        # time_stamps = []
        # dist = []



        # for idx, row in self.trip_df.iterrows():
        #     row_latlon = (float(row['LATITUDE']), float(row['LONGITUDE']))
        #     dist = distance.distance(stop_latlon, row_latlon).m
        #     if dist < 50:

        # Need to get sequence and stop_id...
        # Min dist to stop
