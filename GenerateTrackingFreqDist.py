


"""

GenerateTrackingFreqDist.py

- 2019-11-21 by David Lang
    - Update database with days-to-deliver frequency distribution data from designated conditions of
    CompanyID, ShippedMethod, DateRangeType, StartDate, EndDate, and MaxFreq.

- turnover notes
    - After further project development (to ensure accuracy) update constants' values to be assigned
    by 'sys.argv'.  For cron automation.

"""



####################################################################################################
                                                                                 ###   IMPORTS   ###
                                                                                 ###################

# # KEEP FOR EXPORTING
# import sys
# sys.path.insert(0, '/tomcat/python')

import TrackingSettings as settings
import pandas
from collections import OrderedDict, Counter
from datetime import datetime, timedelta
from Required import Connections



####################################################################################################
                                                                                 ###   GLOBALS   ###
                                                                                 ###################

begin = datetime.now()

conn = Connections.connect()
cur = conn.cursor()



####################################################################################################
                                                                               ###   CONSTANTS   ###
                                                                               #####################

COMPANY_ID      = 1232
SHIPPED_METHOD  = 'USPS Media Mail'
DATE_RANGE_TYPE = 'week'  # <-- Has to be 'day', 'week', 'month', or 'custom'.
START_DATE      = '2019-12-22'
END_DATE        = '2019-12-29'
MAX_FREQ        = 14

TBL_PS_HEADERS = ['TrackingNumber', 'CompletionDate']
TBL_A_HEADERS  = ['MessageTimestamp', 'Delivered']
DF_HEADERS     = TBL_PS_HEADERS + TBL_A_HEADERS

PREFIX_HEADERS = [
    'CompanyID', 'ShippedMethod', 'DateRangeType', 'StartDate', 'EndDate', 'MaxFreq', 'TotalShipped'
]
DAYS_HEADERS      = [ 'Days' + str(i + 1) for i in range(MAX_FREQ - 1) ]
SUFFIX_HEADERS    = ['DaysMaxFreqPlus']
DTD_STATS_HEADERS = PREFIX_HEADERS + DAYS_HEADERS + SUFFIX_HEADERS

SERIES = settings.generate_freq_dist_series

# DEBUG ... For single series manual input.
SERIES = [{
    'company_id': 507, 'shipped_method': 'USPS Media Mail', 'date_range_type': 'week',
    'max_freq': 14
}]



####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    global COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE, MAX_FREQ, START_DATE, END_DATE


    for i, set in enumerate(SERIES):
        COMPANY_ID =        set['company_id']
        SHIPPED_METHOD =    set['shipped_method']
        DATE_RANGE_TYPE =   set['date_range_type']
        MAX_FREQ =          set['max_freq']
        START_DATE, END_DATE = getStartDateAndEndDate()

        mainLoop()

    end = datetime.now()
    exit("\n>>> DONE ... runtime = " + str(end - begin) + "\n\n\n")



def mainLoop():

    print("\n\n>>> retrieving records for:")
    print(">>>    CompanyID     =", COMPANY_ID)
    print(">>>    ShippedMethod =", SHIPPED_METHOD)
    print(">>>    DateRangeType =", DATE_RANGE_TYPE)
    print(">>>    StartDate     =", START_DATE)
    print(">>>    EndDate       =", END_DATE)
    print(">>>    MaxFreq       =", MAX_FREQ)

    exit()

    records = getRecords()

    if records:
        print("\n>>> retrieved", len(records), "records")

        print("\n>>> converting records to dataframe")
        df = convertRecordsToDf(records)

        print("\n>>> calculating 'DaysToDeliver' per record")
        df = addDaysToDeliver(df)

        print("\n>>> calculating frequency distribution of 'DaysToDeliver'")
        freq_dist = getDaysToDeliverFreqDist(df)

    else:
        print("\n>>> no records retrieved ... creating empty frequency distribution")
        freq_dist = createEmptyFreqDist()

    # print("\n>>> inserting into 'tblDaysToDeliverStats'")
    # insertIntoTableDtdStats(freq_dist, len(records))



####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################



def getStartDateAndEndDate():

    start_date_, end_date_  = '', ''

    if DATE_RANGE_TYPE == 'week':
        weekday_int = begin.weekday()
        offset = weekday_int + 1 if weekday_int != 6 else 0
        end_date_ = begin - timedelta(days=offset + MAX_FREQ)
        start_date_ = end_date_ - timedelta(days=7)
        start_date_, end_date_ = [ date.strftime('%Y-%m-%d') for date in [start_date_, end_date_] ]

    elif DATE_RANGE_TYPE == 'month':

        pass

    return start_date_, end_date_



def getRecords():
    """
    input:  constants = TBL_PS_HEADERS, TBL_A_HEADERS, COMPANY_ID, SHIPPED_METHOD, START_DATE,
                        END_DATE
    output: Return list-of-tuples of all records under designated constants' criteria.
    """

    query = """
        SELECT {}
            FROM tblPackageShipments AS ps LEFT JOIN tblArrival AS a
                ON ps.PackageShipmentID = a.PackageShipmentID
            WHERE ps.CompanyID = %s
                AND ps.ShippedMethod = %s
                AND ps.CompletionDate > %s
                AND ps.CompletionDate < %s
    """
    ps_headers = ', '.join([ 'ps.' + h for h in TBL_PS_HEADERS ])
    a_headers = ', '.join([ 'a.' + h for h in TBL_A_HEADERS ])
    query = query.format(ps_headers + ', ' + a_headers)
    cur.execute(query, [COMPANY_ID, SHIPPED_METHOD, START_DATE, END_DATE])
    select_ = cur.fetchall()

    return select_



def convertRecordsToDf(_records):
    """
    input:  _records = List-of-tuples from 'getRecords()'.
    output: Return dataframe object, of data from '_records'.
    """

    converting = [ OrderedDict(zip(DF_HEADERS, row)) for row in _records ]
    converted_ = pandas.DataFrame(converting)

    return converted_



def addDaysToDeliver(_df):
    """
    input:  _df = Dataframe of data from 'getRecords()'.
    output: Return _df with populated 'DayToDeliver' column.
    """

    def getDaysToDeliver(row):
        # Subroutine for dataframe 'apply()'...  When package has been 'Delivered', calculate and
        # populate 'DaysToDeliver' column.
        if row['Delivered'] == 'Y':  return (row['MessageTimestamp'] - row['CompletionDate']).days
        else:  return 'no delivery confirmation'

    _df['DaysToDeliver'] = _df.apply(getDaysToDeliver, axis='columns')

    return _df



def getDaysToDeliverFreqDist(_df):
    """
    input:  constants = MAX_FREQ
            _df =       Dataframe of data from 'getRecords()' with calculated 'DaysToDeliver'.
    output: Return 'freq_dist_', dict with keys as 'DaysToDeliver' frequency classes, and values as
            as the frequencies of the classes.
    """

    # Get raw frequency distribution.
    freq_dist_ = _df['DaysToDeliver'].tolist()
    freq_dist_ = dict(Counter(freq_dist_))

    # Fill in missing values with '0'.
    missing_freqs = { i + 1: 0 for i in range(MAX_FREQ) if i + 1 not in freq_dist_.keys() }
    freq_dist_ = { **freq_dist_, **missing_freqs }
    if 'no delivery confirmation' not in freq_dist_:  freq_dist_['no delivery confirmation'] = 0

    # Trim frequencies to 'MAX_FREQ' value.
    freq_dist_['DaysMaxFreqPlus'] = freq_dist_['no delivery confirmation']
    to_delete = ['no delivery confirmation']
    for k, v in freq_dist_.items():
        if isinstance(k, int) and k >= MAX_FREQ:
            freq_dist_['DaysMaxFreqPlus'] += v
            to_delete += [k]
        # Bug fix ... Sometimes 'DaysToDeliver == 0'.  This is a "garbage in" error.  Pop value from
        # 'freq_dist_[0]' to 'freq_dist_[1]'.
        if k == 0:
            freq_dist_[1] += freq_dist_[0]
            to_delete += [0]
    for deleting in to_delete:  del freq_dist_[deleting]

    # Update 'freq_dist_' keys to align with table columns.
    for k in list(freq_dist_.keys()):
        if k != 'DaysMaxFreqPlus':  freq_dist_['Days' + str(k)] = freq_dist_.pop(k)

    return freq_dist_



def createEmptyFreqDist():
    """ output:  Return dict with keys of DAYS_HEADERS, SUFFIX_HEADERS and values of 0. """
    return { **{ dh: 0 for dh in DAYS_HEADERS }, **{ sh: 0 for sh in SUFFIX_HEADERS } }



def insertIntoTableDtdStats(_freq_dist, _total):
    """
    input:  constants =     DTD_STATS_HEADERS, COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE,
                            START_DATE, END_DATE, DAYS_HEADERS, MAX_FREQ
            _freq_dist =    Dict of frequency distribution calculated from
                            'getDaysToDeliverFreqDist()'.
            _total =        Int of total orders shipped.
    output: Insert into 'tblDaysToDeliverStats' input values.
    """

    # Build 'query'.
    query = """
        INSERT INTO tblDaysToDeliverStats ({}) VALUES ({})
    """
    columns = ', '.join(DTD_STATS_HEADERS)
    markers = ', '.join([ '%s' for i in range(len(DTD_STATS_HEADERS)) ])
    query = query.format(columns, markers)

    # Build 'values' to insert.
    prefix_values = [
        COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE, START_DATE, END_DATE, MAX_FREQ, _total
    ]
    days_values = [ _freq_dist[i] for i in DAYS_HEADERS ]
    suffix_values = [_freq_dist['DaysMaxFreqPlus']]
    values = prefix_values + days_values + suffix_values

    cur.execute(query, values)
    conn.commit()



############
main()   ###
############
