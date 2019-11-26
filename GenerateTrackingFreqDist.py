
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

import sys
sys.path.insert(0, '/tomcat/python')

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

COMPANY_ID = 735

SHIPPED_METHOD  = 'USPS Media Mail'
# Has to be 'day', 'week', 'month', or 'custom'.
DATE_RANGE_TYPE = 'week'
START_DATE      = '2019-11-03'
END_DATE        = '2019-11-10'

MAX_FREQ = 14

TBL_PS_HEADERS = ['TrackingNumber', 'CompletionDate']
TBL_A_HEADERS  = ['MessageTimestamp', 'Delivered']

DF_HEADERS = TBL_PS_HEADERS + TBL_A_HEADERS

PREFIX_HEADERS = [
    'CompanyID', 'ShippedMethod', 'DateRangeType', 'StartDate', 'EndDate', 'MaxFreq', 'TotalShipped'
]
DAYS_HEADERS      = [ 'Days' + str(i + 1) for i in range(MAX_FREQ - 1) ]
SUFFIX_HEADERS    = ['DaysMaxFreqPlus']
DTD_STATS_HEADERS = PREFIX_HEADERS + DAYS_HEADERS + SUFFIX_HEADERS



####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    print("\n\n>>> retrieving records for:")
    print(">>>    CompanyID     =", COMPANY_ID)
    print(">>>    ShippedMethod =", SHIPPED_METHOD)
    print(">>>    DateRangeType =", DATE_RANGE_TYPE)
    print(">>>    StartDate     =", START_DATE)
    print(">>>    EndDate       =", END_DATE)
    print(">>>    MaxFreq       =", MAX_FREQ)

    records = getRecords()
    print("\n>>> retrieved", len(records), "records")

    print("\n>>> converting records to dataframe")
    df = convertRecordsToDf(records)

    print("\n>>> calculating 'DaysToDeliver' per record")
    df = addDaysToDeliver(df)

    print("\n>>> calculating frequency distribution of 'DaysToDeliver'")
    freq_dist = getDaysToDeliverFreqDist(df)

    print("\n>>> inserting into 'tblDaysToDeliverStats'")
    insertIntoTableDtdStats(freq_dist, len(records))

    end = datetime.now()
    exit("\n>>> DONE ... runtime = " + str(end - begin) + "\n\n\n")



####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

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
