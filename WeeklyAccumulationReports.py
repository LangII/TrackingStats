


"""

WeeklyAccumulationReports.py

"""



####################################################################################################
                                                                                 ###   IMPORTS   ###
                                                                                 ###################

from datetime import datetime, timedelta
from Required import Connections
import TrackingSettings as settings
import pandas
from collections import OrderedDict
import statistics
from os import path



####################################################################################################
                                                                                 ###   GLOBALS   ###
                                                                                 ###################

begin = datetime.now()

conn = Connections.connect()
cur = conn.cursor()



####################################################################################################
                                                                               ###   CONSTANTS   ###
                                                                               #####################

""" COMMENT OUT for single manual input """
SERIES = settings.weekly_accumulation_report_series

""" COMMENT IN (and input values) for single manual input """
# SERIES = [{
#     'company_id':       1899,
#     'shipped_method':   'UPS MI Parcel Select',
#     'date_range_type':  'week',
#     'max_freq':         14,
#     # 'start_date':       ''
#     'start_date':       '2019-10-20'
# }]

CSV_PATH = 'prints/tests/'

COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE, MAX_FREQ, START_DATE, CSV_NAME = 0, '', '', 0, '', ''

# Also controls the order by which the carriers are displayed in 'overview_tab'.
CARRIERS = ['UPS', 'USPS', 'DHL', 'FedEx']

COLUMNS = ['CompanyID', 'ShippedMethod', 'StartDate', 'EndDate', 'TotalShipped', 'DaysMaxFreqPlus']
STATS_COLS = ['Mean', 'StDev']



####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    global COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE, MAX_FREQ, START_DATE, COLUMNS, CSV_NAME

    collected_dfs = []
    for index, set in enumerate(SERIES):

        # Get constants per 'set' in SERIES.
        COMPANY_ID =        set['company_id']
        SHIPPED_METHOD =    set['shipped_method']
        DATE_RANGE_TYPE =   set['date_range_type']
        MAX_FREQ =          set['max_freq']
        # Generate START_DATE from current datetime if ['start_date'] is not given.
        START_DATE = set['start_date'] if set['start_date'] else getStartDate()
        # Update COLUMNS with 'Days' columns generated with MAX_FREQ.
        COLUMNS += [ 'Days' + str(i + 1) for i in range(MAX_FREQ - 1) ]
        # Generate CSV_NAME from 'set' constants.
        CSV_NAME = getCsvName()

        print("\n\n\n>>> current set in SERIES:")
        print(">>>    CompanyID     =", COMPANY_ID)
        print(">>>    ShippedMethod =", SHIPPED_METHOD)
        print(">>>    DateRangeType =", DATE_RANGE_TYPE)
        print(">>>    MaxFreq       =", MAX_FREQ)
        print(">>>    StartDate     =", START_DATE)

        print("\n>>> getting statistics")
        stats = getStatistics()

        print("\n>>> converting statistics to dataframe")
        df = convertStatsToDf(stats)

        print("\n>>> updating dataframe with mean and standard deviation")
        df = updateDfWithMeanAndStDev(df)

        # print("\n>>> saving dataframe to csv")
        # saveDfToCsv(df)

        print("\n>>> collecting dataframe")
        collected_dfs += [df]

    print("\n>>> processing collected dataframes")
    processCollectedDfs(collected_dfs)

    run_time = str(datetime.now() - begin)
    exit("\n>>> DONE ... runtime = " + run_time + "\n\n\n")



####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

def getStartDate():
    """
    Return 'start_date_', string of datetime object used in sql query.
    """

    start_date_ = ''

    if DATE_RANGE_TYPE == 'week':
        # Generate 'offset', how many days from most recent Sunday the current date is.  Then
        # generate 'start_date_', string of date representing most recent Sunday, minus 1 week,
        # minus MAX_FREQ.
        offset = begin.weekday() + 1 if begin.weekday() != 6 else 0
        start_date_ = (begin - timedelta(days=offset + MAX_FREQ + 7)).strftime('%Y-%m-%d')

    elif DATE_RANGE_TYPE == 'month':  pass
    elif DATE_RANGE_TYPE == 'custom':  pass
    elif DATE_RANGE_TYPE == 'day':  pass

    return start_date_



def getStatistics():
    """
    input:  constants = COLUMNS, COMPANY_ID, SHIPPED_METHOD, MAX_FREQ, DATE_RANGE_TYPE, START_DATE
    output: Return list-of-tuples of entries from 'tblDaysToDeliverStats' with inserted values from
            constants.
    """

    query = """
        SELECT {} FROM tblDaysToDeliverStats
            WHERE CompanyID = %s
                AND ShippedMethod = %s
                AND MaxFreq = %s
                AND DateRangeType = %s
                AND StartDate >= %s
    """
    query = query.format(', '.join(COLUMNS))
    values = [COMPANY_ID, SHIPPED_METHOD, MAX_FREQ, DATE_RANGE_TYPE, START_DATE]

    cur.execute(query, values)
    select_ = cur.fetchall()

    return select_



def convertStatsToDf(_stats):
    """
    input:  constants = COLUMNS
            _stats = List-of-tuples from 'getStats()'.
    output: Return dataframe object, of data from '_stats'.
    """

    converting = [ OrderedDict(zip(COLUMNS, row)) for row in _stats ]
    converted_ = pandas.DataFrame(converting)

    return converted_



def updateDfWithMeanAndStDev(_df):
    """
    input:  _df = Dataframe from 'convertStatsToDf()'.
    output: Return dataframe with additional calculated rows of 'Mean' and 'StDev'.
    """

    new_columns = _df.columns.tolist() + STATS_COLS
    df_ = pandas.DataFrame(columns=new_columns)

    # The "lazy" method if iterating through a dataframe is used because two different column
    # updates are done using similar calculations.  This way 'one_dim_array' does not need to be
    # generated twice per dataframe row when using dataframe.apply.
    for _, row in _df.iterrows():
        row = dict(row.items())

        # Build 'one_dim_array' needed for calculating 'Mean' and 'StDev'.  if/else conditions for
        # handling rows with 0 packages shipped.
        one_dim_array = []
        if row['TotalShipped'] != 0:
            for i in range(MAX_FREQ - 1):
                one_dim_array.append(row['Days' + str(i + 1)] * [i + 1])
            one_dim_array = sum(one_dim_array, [])
        else:
            one_dim_array = [0, 0]

        # Calculate 'Mean' and 'StDev' and add to dataframe.
        row['Mean'] = round(statistics.mean(one_dim_array), 2)
        row['StDev'] = round(statistics.stdev(one_dim_array), 2)
        df_ = df_.append(row, ignore_index=True)

    return df_



def getCsvName():

    shipped_method = ''.join([ word.lower().title() for word in SHIPPED_METHOD.split() ])
    csv_name_ = '_'.join([str(COMPANY_ID), shipped_method, DATE_RANGE_TYPE, str(MAX_FREQ)]) + '.csv'

    return csv_name_



def saveDfToCsv(_df):

    csv_loc = CSV_PATH + CSV_NAME
    if path.exists(csv_loc):    first_save = False
    else:                       first_save = True

    _df.to_csv(csv_loc, mode='a', index=False, header=first_save)



def processCollectedDfs(_collected_dfs):

    df = pandas.concat(_collected_dfs, ignore_index=True)
    print(""), print(df)
    # df.to_csv('prints/tests/temp.csv')

    """ Subroutine ... Get 'Carrier' from comparing 'ShippedMethod' with 'CARRIERS'. """
    def addCarrier(row):
        for carrier in CARRIERS:
            if row['ShippedMethod'].startswith(carrier):  return carrier
    df['Carrier'] = df.apply(addCarrier, axis='columns')



    # Sort columns.
    cols = df.columns.tolist()
    df = df[cols[:1] + cols[-1:] + cols[1:-1]]

    # BLOCK ... Sort rows.
    df = df.sort_values(by='ShippedMethod')
    # Sort by order of CARRIERS.
    df['Carrier'] = pandas.Categorical(df['Carrier'], CARRIERS)
    df = df.sort_values('Carrier')

    print(""), print(df)



    copy_cols = ['Carrier', 'ShippedMethod', 'StartDate', 'EndDate']
    not_sum_cols = copy_cols + STATS_COLS + ['CompanyID']
    sum_cols = [ col for col in df.columns.tolist() if col not in not_sum_cols ]

    # Create and insert 'totals' rows.
    for sm in df['ShippedMethod'].unique().tolist():

        # Get df of individual shipped methods.
        sm_df = df.loc[df['ShippedMethod'] == sm]

        # Create working totals row by copying first row of 'sm_df'.
        sm_totals_df = sm_df.iloc[[0]].copy()

        # Reset unwanted values of totals row.
        for col in sm_totals_df.columns.tolist():
            if col not in copy_cols:  sm_totals_df[col] = 0

        # Reset 'CompanyID' value.
        sm_totals_df['CompanyID'] = 'TOTALS'

        # Fill totals values with values from 'sm_df' (the ones that just need to be summed).
        for col in sum_cols:  sm_totals_df[col] = sum(sm_df[col].tolist())

        



        # print("")
        # print(sm_df)
        # print("")
        # print(sm_totals_df)

        # break









############
main()   ###
############
