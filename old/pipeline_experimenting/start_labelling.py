import pandas as pd
from datetime import datetime
import time
import numpy as np
import pymongo
from pymongo import MongoClient
from geopy import distance
import random
import string

class Labeling(object):
    """
    Class for labeling raw AVL data with trip_ids
    Data should all be:
        From one distinct GTFS period
        For one route
        In one direction
    """

    def __init__(self, in_collection, out_collection, gtfs_period=0):
        """
        Input:
            in_collection: Collection we are pulling raw data from
            out_collection: Collection we are inserting labeled data into
        """

        self.in_coll = in_collection
        self.out_coll = out_collection

        # Get all unique blocks in the gtfs-specific collection
        self.blocks = self.in_coll.distinct('TRAIN_ASSIGNMENT')

        # Turn these blocks to integers for later lookup
        self.int_blocks = [int(blk) for blk in self.blocks]

        self.get_gtfs_dir(gtfs_period)

        # Run all the setup funtions
        self.setup()


    #########
    # Pipeline Setup
    def setup(self):
        """
        Runs all the setup functions together.
        """

        self.load_filter_gtfs()
        self.find_starting_stop()
        self.check_add_timestamps()

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

    def find_starting_stop(self):
        """
        Takes a sample trip and gets its first stop (the starting stop).
        Gets the stop_id and Latitude/Longitude of the stop
        This is bus-line specific: Eastbound 33 trains all have the same start.
        """

        # Get all scheduled starting stops
        startings = self.sched_trps[self.sched_trps['stop_sequence'] == 1]

        # Get the stop_id from a single sample
        # This only works here because all 33 trips have the same start
        self.strtng_stopid = startings.sample()['stop_id'].values[0]

        # Use that id to find the stop in the class stops DataFrame
        self.strtng_stop = self.stop_sched[self.stop_sched['stop_id'] == \
            self.strtng_stopid]

        # Get the lat/lon of that stop as a tuple
        self.strting_latlon = (self.strtng_stop['stop_lat'].values[0], \
            self.strtng_stop['stop_lon'].values[0])

    def check_add_timestamps(self):
        """
        LEGACY (2018-4-26)
        Shouldn't be necessary when the FTP pipeline is update to add a
        timestamp to each row. Still, it's nice to be careful.
        """

        # Count with 'time_stamp' field, total count
        ts_cnt = self.in_coll.find({ 'time_stamp': { '$exists': True}}).count()
        total_cnt = self.in_coll.find().count()

        # If they are different, all a timestamp to any document lacking one.
        if ts_cnt != total_cnt:

            for doc in self.in_coll.find({ 'time_stamp': { '$exists': False}}):

                object_id = doc['_id']
                time_format = '%m/%d/%Y %H:%M:%S'
                cln_date = datetime.strptime(doc['REPORT_TIME'], time_format)
                time_stamp = cln_date.timestamp()

                update =  {"$set": {'time_stamp':time_stamp}}

                self.in_coll.update_one({'_id':object_id}, update)


    #########
    # Start Detection and Labeling
    def label_single_starts(self):
        """
        For each block:
            Find all rows that are within 20 meters of the starting stop
            Cluster these rows based on time (within 2 minutes of each other)
            From each cluster, get the row that occurs last
            Match this row to a trip's first scheduled departure time
            Add these labeled rows to the out_collection
        """

        start_intersection_count = 0

        # For each block
        for block in self.blocks:

            # Get all intersections with the starting stop
            starts = self.get_all_starts(block)
            start_intersection_count += len(starts)

            # Cluster all these starts in a dictionary
            clusters = self.cluster_starts(starts)

            # Get the latest row from each cluster
            single_starts = self.get_single_starts(clusters)

            # Find the trip_id that matches each start, and update the start row
            labeled_starts = self.get_start_labels(single_starts)

            # Add labeled starts to the output collection
            self.add_to_out_collection(labeled_starts)

            unique_count = len(self.out_coll.find().distinct('trip_id_iso'))
            start_count = self.out_coll.count()

        print ("Total Start Intersection Count: ", start_intersection_count)
        print ("\n")
        print ("Start Count: ", start_count)
        print ("\n")
        print ("Duplicate ID Count: ", unique_count-start_count)
        print ("\n")

    def get_all_starts(self, block):
        """
        For each row, check if it comes within 25 meters of the starting stop
        Input: block_id (as string)
        Output: List of all block intersections
        """
        start_intersections = []

        for doc in self.in_coll.find({'TRAIN_ASSIGNMENT': block}).sort('time_stamp'):

            doc_latlon = (doc['LATITUDE'], doc['LONGITUDE'])

            if distance.distance(self.strting_latlon, doc_latlon).m <= 25:

                    start_intersections.append(doc)

        return start_intersections

    def cluster_starts(self, starts):
        """
        Clusters starting_stop intersections by time (within 2 minutes of any
        row in the cluster)
        Input: List of rows that intersect with the starting_stop
        Output: Dictionary of lists, each list being a cluster (key is irrelevant)
        """

        # Output dictionary
        start_time_clusters = {}

        # For each item in starting-stop intersections
        for idx, item in enumerate(starts):

            # If we are just starting, there are no start-clusters. Create one!
            if len(start_time_clusters) == 0:

                start_time_clusters[idx] = [item]

            else:

                # Convert the reported time to a datetime object
                item_date = datetime.fromtimestamp(item['time_stamp'])

                # Let's check if this row is close to any of the start clusters
                # in our dictionary
                matched = 0

                # For each start cluster in the dictionary
                for key, value in start_time_clusters.items():

                    # For each row (reported momement) in the time cluster
                    for time in value:

                        # Get the time difference between the two moments
                        row_ts = time['time_stamp']
                        row_date = datetime.fromtimestamp(row_ts)
                        time_diff = item_date - row_date

                        # If these two rows occured within 2 minutes of each other
                        if time_diff.total_seconds() < 110:

                            # This row belongs in the cluster! Add it, break the
                            # loop, add to 'matched'
                            start_time_clusters[key].append(item)
                            matched +=1

                            break

                    # Don't match our document to other starting clusters if
                    # it has already been matched

                    if matched != 0:
                        break

                # If this row doesn't match with any existing clusters
                if matched == 0:

                    # Make a new cluster for it!
                    start_time_clusters[idx] = [item]

        return start_time_clusters

    def get_single_starts(self, clusters):
        """
        Gets the last row (by time) from each cluster
        Input: Dictionary of clusters
        Output: List of rows
        """

        single_start_list = []

        for key, value in clusters.items():

            best_time = {'time_stamp':0}

            for item in value:

                if item['time_stamp'] > best_time['time_stamp']:

                    best_time = item

            single_start_list.append(best_time)

        return single_start_list

    def get_start_labels(self, single_starts):
        """
        Match each start to a trip id based on the trip's starting_stop
        departure time. Returns a list of updated documents
        """

        output = []

        for start in single_starts:

            # Get all possible scheduled starts, and a cleaned list of possible
            # departure times
            schedule, departures = self.get_schedule_departs(start)

            # Parse the doc time to match the departure time
            raw_time = (start['REPORT_TIME'].split(" ")[1])
            rawtime_parse = datetime.strptime(raw_time, '%H:%M:%S')

            # Get the difference for each row of the departure time series
            diff_df = departures.apply(lambda x: abs(x-rawtime_parse))

            # Find the actualy scheduled stop using the index of the minimum
            # departure difference
            sched_start = schedule.loc[diff_df.idxmin()]

            # Get the difference in seconds
            time_diff = diff_df.min().seconds

            # Skip potential starts with a scheduled departure time over 30
            # minutes away
            if time_diff > 1800:
                continue

            # Start adding new fields to the documents. Ints have to be cast, to
            # avoid mongoDB errors...
            start['trip_id'] = int(sched_start['trip_id'])
            start['sched_time_diff_seconds'] = int(time_diff)
            start['trip_start'] = int(1)

            # Creat a unique id based on the trip ID and the date of travel
            cln_date = datetime.fromtimestamp(start['time_stamp'])
            iso = cln_date.strftime('%Y-%m-%d')
            rand = ''.join(random.choices(string.ascii_uppercase + string.digits, k=5))

            start['trip_id_iso'] = str(sched_start['trip_id']) + '_' + iso + '_' + rand

            # Getting seconds from noon
            start_dt = datetime.fromtimestamp(start['time_stamp'])
            ssn = (((start_dt.hour * 60) + start_dt.minute) - 720)**2
            start['minutes_noon_sqr'] = ssn

            # Now that we know the trip, get the actual service id
            trip_mask = self.trip_blocks['trip_id'] == start['trip_id']
            trip_trip = self.trip_blocks[trip_mask]['service_id']
            start['service_id'] = int(trip_trip.values[0])

            output.append(start)


        return output


    ##########
    # Detection/Labeling Utilities
    def get_schedule_departs(self, start):
        """
        Get a dataframe of all possible scheduled departures and a cleaned list
        of possible departure times as strings.
        Input: Start Document
        Output: DataFrame of scheduled starts, series of start times
        """

        # Get the service_id of the start based on the day it occured
        # Can be multiple, as buses can be schedule beyond 24 hours (up to
        # 30:34:00!), and these 'late' buses can overlap with early buses
        # the next day
        service_id_lst = self.get_start_service_list(start['time_stamp'])

        # # Bonus! We can add this data to the start itself
        # start['service_id'] = service_id
        # NOPE
        # CAN'T ADD SERVICE ID TILL I FIGURE OUT THE TRIP_ID

        # Get the block_id of the start
        block_id = int(start['TRAIN_ASSIGNMENT'])

        # Get a dataframe of possible, scheduled starts
        sched_starts = self.get_scheduled_starts(block_id, service_id_lst)


        # From our dataframe, get just the departure hours
        dprts = sched_starts['departure_time']

        # Clean the schedule dataframe if there are times greater than 24
        cln_departs = dprts.apply(lambda x: self.clean_schedules(x))

        # Parse the cleaned times
        tm_frmt = '%H:%M:%S'
        prsd_dprts = cln_departs.apply(lambda x: datetime.strptime(x, tm_frmt))

        return sched_starts, prsd_dprts

    def get_start_service_list(self, start_timestamp):
        """
        Lookup a start's service ID for better trip filtering when labeling
        Accounts for gosh-darn crazy schedule hours
        Input: A doc's raw 'reported time', as a string
        Output: A service_id (integer between 1 and 3)
        """

        # Convert the timestamp into a datetime object and get the weekday
        cln_date = datetime.fromtimestamp(start_timestamp)
        wkdy_num = cln_date.weekday()

        # The system-wide max departure time is 30:34 (6:34 the next day)
        # The system-wide min departure time is 02:54 (2:54 this day)
        # Any departure between these hours could be from a schedule on this day
        # or the previous day
        if cln_date.hour <= 7:

            # Careful! Can't have negative weekdays
            if wkdy_num == 0:
                weekdays = list(set([wkdy_num, 6]))
            else:
                weekdays = list(set([wkdy_num, wkdy_num-1]))

        else:
            weekdays = [wkdy_num]

        # Using the GTFS calendar, return the service_id for each possible weekday
        service_ids = []
        for day in weekdays:
            service_row = self.cal_colnum[self.cal_colnum[day] == 1]
            service_ids.append(service_row['service_id'].values[0])

        return service_ids

    def get_scheduled_starts(self, block_id, service_id_list):
        """
        Create a data frame of possible trip start times and id's
        Accounts for multiple starting days do to scheduled trip starts past
        24 hours.
        Input:
            Block id of the start
            List of possible service id's of the start
        Output:
            Dataframe with possible starts
        """

        # Setup masks for filtering all the possible trips
        block_mask = self.trip_blocks['block_id'] == block_id
        service_mask = self.trip_blocks['service_id'].isin(service_id_list)

        # Apply the masks to the trips DataFrame, getting possible trips
        poss_start_trips = self.trip_blocks[block_mask & service_mask]

        # Get all the unique trip_id's from that dataframe
        poss_start_ids = poss_start_trips['trip_id'].unique()

        # Filter the trip_schedule with the trip_id's
        sched_mask = self.sched_trps['trip_id'].isin(poss_start_ids)
        poss_start_sched = self.sched_trps[sched_mask]

        # Get the first stop of all possible trips
        start_mask = poss_start_sched['stop_sequence'] == 1
        poss_start_times = poss_start_sched[start_mask]

        return poss_start_times

    def clean_schedules(self, time_str):
        """
        Given a time string from a GTFS schedule, convert any hours above 24
        into it's actual, real time.
        """

        times = time_str.split(":")
        hour = int(times[0])
        if hour > 24:
            hour = hour-24
        if hour == 24:
            hour = '00'

        return ":".join([str(hour), times[1], times[2]])

    def add_to_out_collection(self, list):
        """
        Takes a list of dictionaries and adds them to the output dictionary
        """

        for doc in list:

            # Upsert, in case the document already exists in the DB
            self.out_coll.update_one({'_id':doc['_id']}, {'$set':doc}, upsert=True)
