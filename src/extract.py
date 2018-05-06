import pandas as pd
from ftplib import FTP
from datetime import datetime
import pymongo
from pymongo import MongoClient

class Extractor(object):

    """
    Class for:
        -Determining Muni data for a given day
        -Extracting and filtering data from the FTP server
        -Loading the data into MongoDB
    """

    def __init__(self, collection, bus='33', direction=0,
                    gtfs_period=0, days=30):

        """
        Input:
            -collection:
                The MongoDB collection into which filter data will be inserted
            -bus:
                The name of the route for which we wish to get data. The names of all
                routes can be found in data/muni_routes.csv
            -direction:
                The direction of travel of our route for which we want data. Usually
                0 or 1
            -gtfs_period:
                Index of the gtfs period we wish to get data for.
                Indices can be looked up in data/gtfs_lookup.csv. The file is
                sorted with most recent periods first
            -days:
                The number of days for which we want data, starting with the most
                recent of the gtfs period first. If None, will get all days
        """

        self.days = days
        self.collection = collection
        self.bus = bus
        self.direction = direction

        # Get the from/to dates of the period and the directory of the gtfs
        # files
        self.get_gtfs_data(gtfs_period)

        self.total_count = 0
        self.filter_count = 0

    ############
    # MAIN METHODS
    ############

    def setup(self):
        """
        Sets up the instance
        """

        # Get the id of the route we want to get data for
        self.get_route_id(bus=self.bus)

        # Get the ids of all trips along this route
        self.get_trip_ids(direction=self.direction)

        # Get the trip block 'names' relevant to our gtfs period and used by
        # trips along our route
        self.get_signid_blocknames()


    def get_insert_data(self):
        """
        Gets the data, filters it, inserts it into the collection
        """

        # Get all file names from the server
        server_files = self.get_server_files()

        # Get the files that fall within our date range
        target_files = self.clean_file_list(server_files)[::-1]

        # Filter by day count if it exists
        if self.days:
            if self.days + 1 < len(target_files):
                target_files = target_files[0:self.days]


        for data_file in target_files:

            file_date = data_file[15:-4]
            print ("Getting data from ", file_date)

            self.connect_read_ftp(data_file)

        print ("Total lines read: ", self.total_count)
        print ("Filtered lines kept: ", self.filter_count)


    def run(self):
        """
        Runs everything
        """
        self.setup()

        self.get_insert_data()

    ############
    # GTFS Setup Tools
    ############

    def get_gtfs_data(self, gtfs_period):
        """
        Sets the date range of the extractor and gtfs directory
        Input:
            gtfs_period: Index of the gtfs_lookup file to use
        """

        gtfs_df = pd.read_csv('data/gtfs_lookup.csv')
        gtfs_series = gtfs_df.iloc[gtfs_period]

        from_txt = gtfs_series['from_date']
        self.from_date = datetime.strptime(from_txt, '%Y-%m-%d')
        self.from_txt = from_txt

        to_txt = gtfs_series['to_date']
        self.to_date = datetime.strptime(to_txt, '%Y-%m-%d')
        self.to_txt = to_txt

        self.gtfs_dir = gtfs_series['directory']

        self.sign_id = gtfs_series['sign_id']

    def get_route_id(self, bus):
        """
        Gets the route ID of the bus route given
        """

        route_txt = 'data/gtfs/{}/routes.txt'.format(self.gtfs_dir)
        routes = pd.read_csv(route_txt)

        # Cleaning route names to make look-up easier
        routes['cln_rts'] = routes['route_short_name'].apply(lambda x: x.strip())

        # Get the routes id for our busline
        self.route_id = routes[routes['cln_rts'] == bus]['route_id'].values[0]

    def get_trip_ids(self, direction):
        """
        Get the ids of all trips associated with the given route
        """

        # Load in the trips
        trip_txt = 'data/gtfs/{}/trips.txt'.format(self.gtfs_dir)
        trips = pd.read_csv(trip_txt)

        # Get all the trips on the route, going in the same direction
        trip_mask = (trips['route_id'] == self.route_id) \
            & (trips['direction_id'] == self.direction)
        bus_trips = trips[trip_mask]

        # Get an array of all the unique block numbers from the 33 trips
        self.trip_blocks = bus_trips['block_id'].unique()

    def get_signid_blocknames(self):
        """
        Get the 'names' of all blocks that correspond to our trips and timeframe
        """

        # Load in the block reference data for connecting blocks in the
        # AVL data to our specific time periods
        blckrf_txt = 'data/lookUpBlockIDToBlockNumNam.csv'
        blockref = pd.read_csv(blckrf_txt)

        # Get all blocks in our time frame
        date_blocks = blockref[blockref['SIGNID'] == self.sign_id]

        # Filter our date-constrained blocks to our trip blocks
        dt_trp_mask = date_blocks['BLOCKNUM'].isin(self.trip_blocks)
        dt_trp_blcks = date_blocks[dt_trp_mask]

        # Finally, get all the block names that correspond to the block numbers
        self.block_names = dt_trp_blcks['BLOCKNAME'].unique()

    ############
    # Get Data Tools
    ############

    def get_server_files(self):
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

    def clean_file_list(self, files):
        """
        Clean and filter filenames from the FTP server
        """

        target_files = []

        for item in files:

            if item[0:11] != 'sfmtaAVLRaw':
                continue

            raw_date = item[15:-4]

            if raw_date[0] == '_':
                continue

            file_time = datetime.strptime(raw_date, '%m%d%Y')

            if file_time >= self.from_date and file_time < self.to_date:

                target_files.append(item)

        return target_files

    def connect_read_ftp(self, file):
        """
        Connect to FTP AVL server, read lines from our file, and pass them to
        the reader.
        Input:
            File: The name of the file to read from the server
        """

        ftp = FTP('avl-data.sfmta.com')
        ftp.login()
        ftp.cwd('AVL_DATA/AVL_RAW/')
        ftp.retrlines('RETR ' + file, self.read_ftp)

    def read_ftp(self, line):
        """
        Read a line from the FTP server, detecting and cleaning the first line
        Passes the line of the filter afterwards
        """

        if len(line) > 125:

            self.header = line[0:89].split(",")
            first = line[89:]

            self.filter_ftp(first)

        else:

            self.filter_ftp(line)

    def filter_ftp(self, line):
        """
        Filter out lines from the FTP file that are blank or have a null value
        at index 7 (TRAIN_ASSIGNMENT column, aka Block Name)
        """

        if len(line) != 0:

            self.total_count += 1

            ln_splt = line.split(",")

            if ln_splt[7] in self.block_names:

                self.filter_count += 1

                self.dict_db_insert(ln_splt)


    def dict_db_insert(self, line_list):
        """
        Given a split line of data, zip it to headers, turn it into a dictionary
        and insert it in the database.
        Input: A comma-split line of AVL data
        """

        line_dict = {}

        # Format of injesting time strings
        time_format = '%m/%d/%Y %H:%M:%S'

        # Convert the reported time to a datetime object
        cln_date = datetime.strptime(line_list[1], time_format)

        # Add a timestamp to our data for easier sorting
        line_dict['time_stamp'] = cln_date.timestamp()

        for key, val in zip(self.header, line_list):
            line_dict[key] = val

        self.collection.insert_one(line_dict)
