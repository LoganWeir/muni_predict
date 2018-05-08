from datetime import datetime
import pandas as pd
import numpy as np

import pymongo
from pymongo import MongoClient

def temporal_features_total(trip_id_list, trip_collection, output_collection):

    for idx, trip in enumerate(trip_id_list):

        print ("Getting total duration data for ", trip)
        print ("Number ", idx+1, " of ", len(trip_id_list))

        start_search = {
                'trip_id_iso': trip,
                'trip_start': 1
            }

        end_search = {
                'trip_id_iso': trip,
                'trip_end': 1
            }

        trip_start = trip_collection.find_one(start_search)
        trip_end = trip_collection.find_one(end_search)

        if trip_start and trip_end:

            trip_dict = {}

            start_ts = trip_start['time_stamp']
            end_ts = trip_end['time_stamp']

            trip_dict['start_timestamp'] = start_ts
            trip_dict['trip_id_iso'] = trip

            trip_duration = end_ts - start_ts
            trip_dict['duration'] = trip_duration

            trip_dict['min_noon_sqr'] = trip_start['minutes_noon_sqr']

            # Minutes since midnight
            start_dt = datetime.fromtimestamp(start_ts)
            msm = (start_dt.hour * 60) + start_dt.minute
            trip_dict['min_since_midnight'] = msm


            output_collection.insert_one(trip_dict)


def chunk_data_interval(trip_id_list, trip_collection, chunk_collection,
                        output_collection, chunk_interval):

    cnk_info = chunk_collection.find_one({'number_chunks':chunk_interval})

    for idx, trip in enumerate(trip_id_list):

        trip_data = {}

        print ("Getting ", chunk_interval, " Chunk data for ", trip)
        print ("Number ", idx+1, " of ", len(trip_id_list))

        start_search = {
                'trip_id_iso': trip,
                'trip_start': 1
            }

        trip_start = trip_collection.find_one(start_search)

        trip_data['start_timestamp'] = trip_start['time_stamp']
        trip_data['trip_id_iso'] = trip

        breakin = 0

        for chnk_seq, chnk_data in cnk_info['chunks'].items():

            field = "chunk_" + str(chunk_interval)

            search = {
                'trip_id_iso': trip,
                field: chnk_seq
            }

            chnk_str = '_chnk_' + chnk_seq

            chnk_cursor = trip_collection.find(search)

            chnk_df = pd.DataFrame(list(chnk_cursor))

            min_ts = chnk_df['time_stamp'].min()
            max_ts = chnk_df['time_stamp'].max()

            chnk_secs = max_ts - min_ts

            trip_data['seconds' + chnk_str ] = chnk_secs

            min_dt = datetime.fromtimestamp(min_ts)
            mfn_sq = (((min_dt.hour * 60) + min_dt.minute) - 720)**2

            trip_data['mfn_sq' + chnk_str] = mfn_sq

            avg_spd = chnk_df['SPEED'].astype('float').mean()

            trip_data['avg_speed' + chnk_str] = avg_spd

        if breakin == 0:

            output_collection.insert_one(trip_data)


def six_chunk_data(trip_id_list, trip_collection, chunk_collection, output_collection):

    six_cnk_info = chunk_collection.find_one({'number_chunks':6})

    for idx, trip in enumerate(trip_id_list):

        print ("Getting Two-Chunk data for ", trip)
        print ("Number ", idx+1, " of ", len(trip_id_list))

        trip_data = {}

        breakin = 0

        for chnk_seq, chnk_data in six_cnk_info['chunks'].items():

            search = {
                'trip_id_iso': trip,
                'chunk_6': chnk_seq
            }

            chnk_str = '_chnk_' + chnk_seq

            chnk_cursor = trip_collection.find(search)

            if chnk_cursor.count() == 0:
                breakin += 1
                break

            chnk_df = pd.DataFrame(list(chnk_cursor))

            min_ts = chnk_df['time_stamp'].min()
            max_ts = chnk_df['time_stamp'].max()
            chnk_secs = max_ts - min_ts

            if chnk_secs > 1500:
                breakin += 1
                break

            trip_data['seconds' + chnk_str ] = chnk_secs

            min_dt = datetime.fromtimestamp(min_ts)
            mfn_sq = (((min_dt.hour * 60) + min_dt.minute) - 720)**2
            trip_data['mfn_sq' + chnk_str] = mfn_sq

            avg_spd = chnk_df['SPEED'].astype('float').mean()

            trip_data['avg_speed' + chnk_str] = avg_spd

        if breakin == 0:

            output_collection.insert_one(trip_data)
