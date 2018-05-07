import pandas as pd
# Turn off the pandas chained assignment warning
pd.options.mode.chained_assignment = None

from geopy import distance

import pymongo
from pymongo import MongoClient

def get_dist(row, stop_tuple):
    """
    Function for finding the distance between a sequential stop and
    shape points along the route.
    Intended to be applied to the shapes DataFrame
    """

    shape_lat = float(row['shape_pt_lat'])
    shape_lon = float(row['shape_pt_lon'])
    return distance.distance((shape_lat, shape_lon), stop_tuple).m


def create_sample_schedule(gtfs_period, trip_collection):

    # Load in all the data
    trip_ids = trip_collection.distinct('trip_id')

    gtfs_lkup = pd.read_csv('data/gtfs_lookup.csv')
    gtfs_dir = gtfs_lkup.iloc[gtfs_period]['directory']

    shapes_txt = 'data/gtfs/{}/shapes.txt'.format(gtfs_dir)
    shapes = pd.read_csv(shapes_txt)

    sched_txt = 'data/gtfs/{}/stop_times.txt'.format(gtfs_dir)
    sched = pd.read_csv(sched_txt)

    trips_txt = 'data/gtfs/{}/trips.txt'.format(gtfs_dir)
    trips = pd.read_csv(trips_txt)

    stops_txt = 'data/gtfs/{}/stops.txt'.format(gtfs_dir)
    stops = pd.read_csv(stops_txt)

    # Get a sample trip with the longest route possible
    sched_trips = sched[sched['trip_id'].isin(trip_ids)]
    seq_max_idx = sched_trips['stop_sequence'].idxmax()
    samp_trip_id = sched_trips.loc[seq_max_idx]['trip_id']

    # Get the schedule of our sample trip, and get the columns we want
    samp_sched = sched_trips[sched_trips['trip_id'] == samp_trip_id]
    small_sched = samp_sched[['stop_id', 'stop_sequence']]
    small_sched.reset_index(drop=True, inplace=True)
    small_sched['seq_str'] = small_sched['stop_sequence'].astype(str);

    # Lookup the shape ID of our sample trip
    shape_id = trips[trips['trip_id'] == samp_trip_id]['shape_id'].values[0]
    trip_shape = shapes[shapes['shape_id'] == shape_id]

    # Iterate over our sample schedule, getting info for each row
    stops_distances = []
    stop_lat = []
    stop_lon = []
    stop_name = []

    for idx, stp_srs in small_sched.iterrows():

        # Get the details of the stops
        stp_id = stp_srs['stop_id']
        stp_dtls = stops[stops['stop_id'] == stp_id]

        stop_name.append(stp_dtls['stop_name'].values[0])

        # Get the stop lat/lon
        stp_lat = float(stp_dtls['stop_lat'])
        stp_lon = float(stp_dtls['stop_lon'])
        stop_lat.append(stp_lat)
        stop_lon.append(stp_lon)
        stop_coords = (stp_lat, stp_lon)

        # Get the distance from the stop to each shape. Choose the closest.
        trip_shape['stop_dist'] = trip_shape.apply\
            (lambda row: get_dist(row, stop_coords), axis=1)
        closest_idx = trip_shape['stop_dist'].idxmin()
        stops_distances.append\
            (trip_shape.loc[closest_idx]['shape_dist_traveled'])

    # Assign this values to columns in our small DataFrame
    small_sched['stop_distance'] = stops_distances
    small_sched['stop_lat'] = stop_lat
    small_sched['stop_lon'] = stop_lon
    small_sched['stop_name'] = stop_name

    small_sched.to_csv('data/scheduled_stop_info.csv', index=False)
