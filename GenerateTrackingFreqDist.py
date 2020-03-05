


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
from Required import Connections, Mail



####################################################################################################
                                                                                 ###   GLOBALS   ###
                                                                                 ###################

begin = datetime.now()

conn = Connections.connect()
cur = conn.cursor()



####################################################################################################
                                                                               ###   CONSTANTS   ###
                                                                               #####################

COMPANY_ID      = 0
SHIPPED_METHOD  = ''
DATE_RANGE_TYPE = ''
START_DATE      = ''
END_DATE        = ''
MAX_FREQ        = 0

TBL_PS_HEADERS = ['TrackingNumber', 'CompletionDate']
TBL_A_HEADERS  = ['MessageTimestamp', 'Delivered']
DF_HEADERS     = TBL_PS_HEADERS + TBL_A_HEADERS

PREFIX_HEADERS = [
    'CompanyID', 'ShippedMethod', 'DateRangeType', 'StartDate', 'EndDate', 'MaxFreq', 'TotalShipped'
]

EMAIL_PACKAGE = {'totals': [], 'run_time': ''}

EMAIL_TO = ['dlang@disk.com']
EMAIL_FROM = 'dlang@disk.com'
EMAIL_SUBJECT = 'GenerateTrackingFreqDist.py recap'

""" Pulls SERIES from TrackingSettings.py for looping through multiple value sets. """
SERIES = settings.generate_freq_dist_series

""" DEBUG ... For single series manual input. """
### If ['start_date'] and ['end_date'] are empty, script will auto fill with most recent Sundays
### assuming 'date_range_type' is 'week'.
# SERIES = [{
#     'company_id': 816, 'shipped_method': 'UPS SURE POST Over 1LB',
#     'date_range_type': 'week', 'max_freq': 14, 'start_date': '', 'end_date': ''
# }]



####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    global COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE, MAX_FREQ, START_DATE, END_DATE
    global DAYS_HEADERS, SUFFIX_HEADERS, DTD_STATS_HEADERS, EMAIL_PACKAGE

    for i, set in enumerate(SERIES):

        # Reset constants per 'set' in SERIES.
        COMPANY_ID =        set['company_id']
        SHIPPED_METHOD =    set['shipped_method']
        DATE_RANGE_TYPE =   set['date_range_type']
        MAX_FREQ =          set['max_freq']
        if not set['start_date'] and not set['end_date']:
            START_DATE, END_DATE =  getStartDateAndEndDate()
        else:
            START_DATE, END_DATE = set['start_date'], set['end_date']
        DAYS_HEADERS      = [ 'Days' + str(i + 1) for i in range(MAX_FREQ - 1) ]
        SUFFIX_HEADERS    = ['DaysMaxFreqPlus']
        DTD_STATS_HEADERS = PREFIX_HEADERS + DAYS_HEADERS + SUFFIX_HEADERS

        mainLoop()

    run_time = str(datetime.now() - begin)
    EMAIL_PACKAGE['run_time'] = run_time

    print("\n\n\n>>> sending recap email ...")
    sendRecapEmail(EMAIL_PACKAGE)

    exit("\n>>> DONE ... runtime = " + run_time + "\n\n\n")



def mainLoop():

    global EMAIL_PACKAGE

    print("\n\n\n>>> retrieving records for:")
    print(">>>    CompanyID     =", COMPANY_ID)
    print(">>>    ShippedMethod =", SHIPPED_METHOD)
    print(">>>    DateRangeType =", DATE_RANGE_TYPE)
    print(">>>    StartDate     =", START_DATE)
    print(">>>    EndDate       =", END_DATE)
    print(">>>    MaxFreq       =", MAX_FREQ)

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
        print("\n>>> NO RECORDS RETRIEVED ... creating empty frequency distribution")
        freq_dist = createEmptyFreqDist()

    print("\n>>> inserting into 'tblDaysToDeliverStats'")
    insertIntoTableDtdStats(freq_dist, len(records))

    print("\n>>> building email package")
    loop_pack = [str(COMPANY_ID), SHIPPED_METHOD, DATE_RANGE_TYPE, str(len(records))]
    EMAIL_PACKAGE['totals'] += [loop_pack]

    completed_print = "\n>>> COMPLETED retrieval and insertion of {} orders for {}, {}, {}ly"
    completed_insert = [str(len(records)), COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE]
    print(completed_print.format(*completed_insert))



####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

def getStartDateAndEndDate():
    """
    Return 'start_date_' and 'end_date_', generated datetime objects used in sql query.
    """

    start_date_, end_date_  = '', ''

    # With 'week' DATE_RANGE_TYPE, 'start_date_' and 'end_date_' are two consecutive Sundays, where
    # 'end_date_' is more than MAX_FREQ from current date.
    if DATE_RANGE_TYPE == 'week':
        weekday_int = begin.weekday()
        offset = weekday_int + 1 if weekday_int != 6 else 0
        end_date_ = begin - timedelta(days=offset + MAX_FREQ)
        start_date_ = end_date_ - timedelta(days=7)
        start_date_, end_date_ = [ date.strftime('%Y-%m-%d') for date in [start_date_, end_date_] ]

    # FUTURE DEV ... Auto generate START_DATE and END_DATE for other DATE_RANGE_TYPE options.
    elif DATE_RANGE_TYPE == 'month':  pass
    elif DATE_RANGE_TYPE == 'custom':  pass
    elif DATE_RANGE_TYPE == 'day':  pass

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



def sendRecapEmail(_email_package):
    """
    input:  constants =         EMAIL_TO, EMAIL_FROM, EMAIL_SUBJECT
            _email_package =    Package built in 'main()' with keys 'totals', 'errors', and
                                'run_time'.  'totals' are converted to string 'email_message' for
                                email message, 'errors' are converted to 'email_df' for email file,
                                and 'run_time' is a timedelta of the script's run time.
    output: Send email to EMAIL_TO of recap of 'totals' and 'run_time' produced by script.
    """

    # Build 'email_message' from '_email_package' before sending email.
    email_message =  '\nrecap of generated tracking frequency distribution statistics\n\n'
    email_message += '-----\ncompany id / shipped method / date range type / packages shipped :\n'
    for total in _email_package['totals']:  email_message += '\n    ' + ' / '.join(total)
    email_message += '\n-----\n\nscript runtime:  ' + _email_package['run_time']

    Mail.SendInfusion(EMAIL_TO, EMAIL_FROM, email_message, EMAIL_SUBJECT)



############
main()   ###
############
