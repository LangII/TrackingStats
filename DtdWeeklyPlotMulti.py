
################################################
"""   <><><>   UNDER CONSTRUCTION   <><><>   """
################################################

"""

DtdWeeklyPlotMulti.py

- turnover notes ...
    - After further project development (to ensure accuracy) update constants' values to be assigned
    by 'sys.argv'.  For cron automation.

"""

####################################################################################################
                                                                                 ###   IMPORTS   ###
                                                                                 ###################

from datetime import datetime
from Required import Connections
from collections import OrderedDict
import pandas
import statistics
import matplotlib.pyplot as plt

####################################################################################################
                                                                                 ###   GLOBALS   ###
                                                                                 ###################

begin = datetime.now()

conn = Connections.connect()
cur = conn.cursor()

####################################################################################################
                                                                               ###   CONSTANTS   ###
                                                                               #####################

COMPANY_IDS     = [507, 1603, 735, 1900, 657]
SHIPPED_METHOD  = 'USPS Media Mail'
# COMPANY_IDS     = [1899]
# SHIPPED_METHOD  = 'UPS MI BPM'
DATE_RANGE_TYPE = 'week'
GT_ET_DATE      = '2019-10-06'
LT_ET_DATE      = '2019-11-17'
MAX_FREQ        = 14

COLUMNS = ['CompanyID', 'StartDate', 'EndDate', 'TotalShipped', 'DaysMaxFreqPlus']
DAYS_COLS = [ 'Days' + str(i + 1) for i in range(MAX_FREQ) ][:-1]
COLUMNS = COLUMNS + DAYS_COLS

####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    stats = getStats()
    df = convertStatsToDf(stats)
    df = updateDfWithMeanAndStDev(df)
    print(df)

    df.to_csv('df01.csv', index=False)

    # generatePlots(df)

    end = datetime.now()
    exit("\n>>> DONE ... runtime = " + str(end - begin) + "\n\n\n")

####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

def getStats():
    """
    input:  constants = COLUMNS, COMPANY_IDS, SHIPPED_METHOD, MAX_FREQ, DATE_RANGE_TYPE, GT_ET_DATE,
                        LT_ET_DATE
    output: Return 'select_', a list-of-lists of data from input conditionals.
    """

    query = """
        SELECT {} FROM tblDaysToDeliverStats
            WHERE CompanyID IN({})
                AND ShippedMethod = %s
                AND MaxFreq = %s
                AND DateRangeType = %s
                AND StartDate >= %s
                AND StartDate <= %s
    """
    query = query.format(', '.join(COLUMNS), ', '.join([ str(id) for id in COMPANY_IDS ]))
    values = [SHIPPED_METHOD, MAX_FREQ, DATE_RANGE_TYPE, GT_ET_DATE, LT_ET_DATE]

    cur.execute(query, values)
    select_ = [ list(i) for i in cur.fetchall() ]

    return select_



def convertStatsToDf(_stats):
    """
    input:  _stats = List-of-tuples from 'getStats()'.
    output: Return dataframe object, of data from '_stats'.
    """

    converted_ = [ OrderedDict(zip(COLUMNS, row)) for row in _stats ]
    converted_ = pandas.DataFrame(converted_)

    # Sort dataframe before returning.
    converted_.sort_values(by=['StartDate', 'CompanyID'], inplace=True)
    converted_ = converted_.reset_index(drop=True)

    return converted_



def updateDfWithMeanAndStDev(_df):
    """
    input:  _df = Dataframe from 'convertStatsToDf()'.
    output: Return dataframe with additional calculated rows of 'Mean' and 'StDev'.
    """

    # The "lazy" method if iterating through a dataframe is used because two different column
    # updates are done using similar calculations.  This way 'one_dim_array' does not need to be
    # generated twice per dataframe row when using dataframe.apply.

    new_columns = _df.columns.tolist() + ['Mean', 'StDev']
    df_ = pandas.DataFrame(columns=new_columns)

    for _, row in _df.iterrows():
        row = dict(row.items())

        one_dim_array = []
        for i in range(MAX_FREQ - 1):
            one_dim_array.append(row['Days' + str(i + 1)] * [i + 1])
        one_dim_array = sum(one_dim_array, [])

        row['Mean'] = round(statistics.mean(one_dim_array), 2)
        row['StDev'] = round(statistics.stdev(one_dim_array), 2)
        df_ = df_.append(row, ignore_index=True)

    return df_



def generatePlots(_df):

    fig, (dtd, packages) = plt.subplots(nrows=2, ncols=1, sharex=True)
    shared_x, dtd_plots, packages_plots = [], [], []

    for comp in COMPANY_IDS:

        dtd_plot = []
        packages_plot = []

        for _, row in _df.iterrows():
            row = dict(row.items())

            # Only deal with 1 'CompanyID' at a time.
            if row['CompanyID'] != comp:  continue

            # Generate 'x_date' for all plots.
            if COMPANY_IDS.index(comp) == 0:
                start_date = row['StartDate'].strftime('%m-%d')
                end_date = row['EndDate'].strftime('%m-%d')
                shared_x.append(start_date + ' to ' + end_date)

            dtd_plot.append(row['Mean'])
            packages_plot.append(row['TotalShipped'] - row['DaysMaxFreqPlus'])

        dtd_plots.append(dtd_plot)
        packages_plots.append(packages_plot)

    for i, comp in enumerate(COMPANY_IDS):
        dtd.plot(shared_x, dtd_plots[i], label=comp)
        packages.plot(shared_x, packages_plots[i])

    dtd.legend(loc='upper left', bbox_to_anchor=(1, 1))
    dtd.set_ylabel('# of days to deliver')
    packages.set_ylabel('# of packages')
    packages.set_xlabel('date range (by week)')
    plt.xticks(rotation=30, ha='right')
    fig.tight_layout()
    plt.show()



############
main()   ###
############
