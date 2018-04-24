import pandas as pd
from ftplib import FTP
from datetime import datetime

def get_data_list():
    """
    Input: None
    Output: List of all files available on STMTA FTP Server
    """
    ftp = FTP('avl-data.sfmta.com')
    ftp.login()
    ftp.cwd('AVL_DATA/AVL_RAW/')
    files = []
    ftp.retrlines('NLST', files.append)

    return files


def clean_sort_file_list(file_list):

    """
    Input: A list of FTP file
    Output: A list of csv file dates, sorted with newest first
    """

    clean_dates = []

    for item in file_list:

        if item[0:11] != 'sfmtaAVLRaw':
            continue

        raw_date = item[15:-4]

        if raw_date[0] == '_':
            continue

        file_time_raw = datetime.strptime(raw_date, '%m%d%Y')

        clean_dates.append(file_time_raw)

    clean_dates.sort(reverse=True)

    return clean_dates


def get_gtfs_file(datetime, gtfs_lookup_df):

    for gtfs_file in gtfs_lookup_df.iterrows():

        from_raw = gtfs_file[1]['from_date']
        from_date = datetime.strptime(from_raw, '%Y-%m-%d')

        to_raw = gtfs_file[1]['to_date']
        to_date = datetime.strptime(to_raw, '%Y-%m-%d')

        if from_date <= datetime < to_date:

            return gtfs_file[1]['directory']

    return None


def build_df(clean_list, gtfs_lookup_df):

    """
    Input:
        -List of clean, sorted files constrained by date
        -Dataframe of GTFS directory and their effective date ranges
    Output:
        DataFrame with
            -Timestamp of Data Date
            -ISO string of Data Date
            -String of FTP file name
            -String of local GTFS Data Location
    """

    pre_df = []

    for item in clean_list:

        ftp_filename = 'sfmtaAVLRawData' + item.strftime('%m%d%Y') + '.csv'

        iso_string = item.strftime('%Y-%m-%d')

        time_stamp = item.timestamp()

        directory = get_gtfs_file(item, gtfs_lookup_df)

        pre_df.append([ftp_filename, iso_string, time_stamp, directory])

    df = pd.DataFrame(pre_df, columns=['ftp_filename', 'iso_string', 'time_stamp', 'gtfs_directory'])

    return df



def x_recent_days(num_days):

    """
    Input: Number of Days of Data Desired
    Output: DataFrame file, time and gtfs data

    MAX DAYS AVAILABLE: 1529
    """

    gtfs_lookup_df = pd.read_csv('data/gtfs_lookup.csv')

    files = get_data_list()

    cln_srted = clean_sort_file_list(files)

    selection = cln_srted[:num_days]

    df = build_df(selection, gtfs_lookup_df)

    return df
