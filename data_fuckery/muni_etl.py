import pandas as pd
from ftplib import FTP
from datetime import datetime
import pymongo
from pymongo import MongoClient

class MuniETL(object):

    """
    Class for:
        -Determining Muni data for a given day
        -Extracting and filtering data from the FTP server
        -Loading the data into MongoDB
    """

    def __init__(self, file_date_series, blockref, signref, db_name, col_name):

        """
        Input:
            -file_date_series: A pandas series contained date and gtfs data
                for the file we are extracting from the FTP server
            -blockref: Block Reference. A dataframe that connects BLOCKNUM
                (from the GTFS data) to BLOCKNAME (from the AVL data) with
                respect to a time period connected to the SIGNID Column. Loaded
                from a csv, which pulled from the AVL FTP repository
            -signref: Sign Reference. A dataframe that connects a SIGNID to a
                date range (FROMDATE, TODATE). Loaded from a csv, which pulled
                from the AVL FTP repository
            -db_name: The name of the database in which we will make our
                collection
            -col_name: The of the collection, into which we will insert our data
                as documents
        """

        # Load data from the file_date_series into the class
        # Date of data as a DateTime object
        self.datetime = datetime.fromtimestamp\
            (file_date_series['time_stamp'].values[0])
        # Location of the local GTFS directory, containing the data model of the
        # SFMTA system, including Routes and their Trips
        self.gtfs_directory = file_date_series['gtfs_directory'].values[0]
        # Filename of the AVL csv on the FTP server
        self.ftp_filename = file_date_series['ftp_filename'].values[0]

        # Loading in the BlockRef DataFrame
        self.blockref = blockref
        # Loading, sorting the SignRef DataFrame
        self.signref = signref
        self.signref.sort_values(by=['FROMDATE', 'TODATE'], ascending=False)

        # Loading in the routes from the local GTFS directory
        # Routes contain the name of the bus line, along with it's route_id
        route_txt = 'data/gtfs/{}/routes.txt'.format(self.gtfs_directory)
        self.routes = pd.read_csv(route_txt)
        # Cleaning route names to make look-up easier
        cln_route = self.routes['route_short_name'].apply(lambda x: x.strip())
        self.routes['cln_rts'] = cln_route

        # Loading in the trips from the local GTFS directory
        # Each trip is one run of a bus along a specific route, in a given
        # direction.
        trip_txt = 'data/gtfs/{}/trips.txt'.format(self.gtfs_directory)
        self.trips = pd.read_csv(trip_txt)


        # Connecting to the database
        self.client = MongoClient('localhost', 27017)
        self.db = self.client[db_name]
        self.collection = self.db[col_name]

    def get_sign_id_blocks(self):
        """
        Find the sign_id that corresponds to the date of the data
        Get all blocks with that given sign_id
        """
        sign_row_1 = self.signref[self.signref['FROMDATE'] < self.datetime]
        sign_row_2 = self.signref[self.signref['TODATE'] > self.datetime]

        # If the data is older that the newest sign_id, just grab the most
        # recent sign_id
        if sign_row_2.shape[0] == 0:
            self.sign_id = sign_row_1.loc[0]['SIGNID']
        else:
            self.sign_id = sign_row_2['SIGNID']

        self.date_blocks = self.blockref[self.blockref\
            ['SIGNID'] == self.sign_id]

    def get_muni_data(self, bus='33', direction=0):
        """
        Get the BlockNames of the BlockID's within our sign_id time-frame that
        are connected to our route's trips.
        Input:
            -The route_short_name of the bus we are interested in
            -The direction we are interested. With the 33, direction=0 means
                a geneal western direction, starting in the Richmond and ending
                in the Mission.
        """

        # Get the routes id for our busline
        rout_srs = self.routes[self.routes['cln_rts'] == bus]
        self.route_id = rout_srs['route_id'].values[0]

        # Get all the trips on the route, going in the same direction
        trip_mask = (self.trips['route_id'] == self.route_id) \
            & (self.trips['direction_id'] == 0)

        self.bus_trips = self.trips[trip_mask]

        # Get an array of all the unique block numbers from the 33 trips
        self.trip_blocks = self.bus_trips['block_id'].unique()

        # Filter our date-constrained blocks to our trip blocks
        self.dateblock_bus = self.date_blocks[\
            self.date_blocks['BLOCKNUM'].isin(self.trip_blocks)]

        # Finally, get all the block names that correspond to the block numbers
        self.dateblock_names = self.dateblock_bus['BLOCKNAME'].values

    def dict_db_insert(self, line_list):
        """
        Given a split line of data, zip it to headers, turn it into a dictionary
        and insert it in the database.
        Input: A comma-split line of AVL data
        """

        line_dict = {}

        for key, val in zip(self.header, line_list):
            line_dict[key] = val

        self.collection.insert_one(line_dict)

    def filter_ftp(self, line):
        """
        Filter out lines from the FTP file that are blank or have a null value
        at index 7 (TRAIN_ASSIGNMENT column, aka Block Name)
        """

        if len(line) != 0:

            ln_splt = line.split(",")

            if ln_splt[7] in self.dateblock_names:

                self.dict_db_insert(ln_splt)


    def read_ftp(self, line):
        """
        Read a line from the FTP server, detecting and cleaning the first line
        """

        if len(line) > 125:

            self.header = line[0:89].split(",")
            first = line[89:]

            self.filter_ftp(first)

        else:

            self.filter_ftp(line)


    def connect_ftp(self):
        """
        Connect to FTP AVL server, read lines from our file, and pass them to
        the reader.
        """

        ftp = FTP('avl-data.sfmta.com')
        ftp.login()
        ftp.cwd('AVL_DATA/AVL_RAW/')
        ftp.retrlines('RETR ' + self.ftp_filename, self.read_ftp)
