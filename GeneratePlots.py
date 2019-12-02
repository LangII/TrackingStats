
################################################
"""   <><><>   UNDER CONSTRUCTION   <><><>   """
################################################

import pandas
import statistics
import matplotlib.pyplot as plt
# import matplotlib.dates as mdates
from datetime import datetime
from collections import OrderedDict
from Required import Connections

begin = datetime.now()

conn = Connections.connect()
cur = conn.cursor()

# SINGLE = [1899, 'UPS MI BPM']

MULTI = [
    [1899, 'UPS MI BPM'],       [507 , 'USPS Media Mail'],
    [1603, 'USPS Media Mail'],  [735 , 'USPS Media Mail']
]

VALUES = {
    'company_id': 507,
    'shipped_method': 'USPS Media Mail',
    'max_freq': 14,
    'date_range_type': 'week',
    'greater_date': '2019-10-06',
    'less_date': '2019-11-10'
}

COLUMNS = ['CompanyID', 'StartDate', 'EndDate', 'TotalShipped', 'DaysMaxFreqPlus']
DAYS_COLS = [ 'Days' + str(i + 1) for i in range(VALUES['max_freq']) ][:-1]
COLUMNS = COLUMNS + DAYS_COLS

####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    # stats = {}
    # for company_id, shipped_method in GROUPS:
    #     VALUES['company_id'], VALUES['shipped_method'] = company_id, shipped_method
    #     group_stats = getStatsByGroup(VALUES)
    #     stats[company_id] = convertStatsToDf(group_stats)
    # for group in stats:  print(stats[group])
    # df = updateDfWithMeanAndStDev(stats[1899])

    stats = getStats(VALUES)
    df = convertStatsToDf(stats)
    df = updateDfWithMeanAndStDev(df)
    print(df)

    generatePlots(df)

    end = datetime.now()
    exit("\n>>> DONE ... runtime = " + str(end - begin) + "\n\n\n")

####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

def getStats(_values):

    query = """
        SELECT {} FROM tblDaysToDeliverStats
            WHERE CompanyID = %s
                AND ShippedMethod = %s
                AND MaxFreq = %s
                AND DateRangeType = %s
                AND StartDate >= %s
                AND StartDate <= %s
    """
    query = query.format(', '.join(COLUMNS))
    values = [
        _values['company_id'], _values['shipped_method'], _values['max_freq'],
        _values['date_range_type'], _values['greater_date'], _values['less_date']
    ]

    cur.execute(query, values)
    select_ = cur.fetchall()

    return select_



def convertStatsToDf(_stats):
    """
    input:  _stats = List-of-tuples from 'getStatsByGroup()'.
    output: Return dataframe object, of data from '_stats'.
    """

    converting = [ OrderedDict(zip(COLUMNS, row)) for row in _stats ]
    converted_ = pandas.DataFrame(converting)

    return converted_



def updateDfWithMeanAndStDev(_df):

    # The "lazy" method if iterating through a dataframe is used because two different column
    # updates are done using similar calculations.  This way 'one_dim_array' does not need to be
    # generated twice per dataframe row when using dataframe.apply.

    new_columns = _df.columns.tolist() + ['Mean', 'StDev']
    df_ = pandas.DataFrame(columns=new_columns)

    for _, row in _df.iterrows():
        row = dict(row.items())

        one_dim_array = []
        for i in range(VALUES['max_freq'] - 1):
            one_dim_array.append(row['Days' + str(i + 1)] * [i + 1])
        one_dim_array = sum(one_dim_array, [])

        row['Mean'] = round(statistics.mean(one_dim_array), 2)
        row['StDev'] = round(statistics.stdev(one_dim_array), 2)
        df_ = df_.append(row, ignore_index=True)

    return df_



def generatePlots(_df):

    # LOCAL VARIABLES...
    # df_range = range(len(_df.index))
    # mean, stdev = _df['Mean'].tolist(), _df['StDev'].tolist()
    # start_dates = [ i.strftime('%m-%d') for i in _df['StartDate'].tolist() ]
    # end_dates = [ i.strftime('%m-%d') for i in _df['EndDate'].tolist() ]
    # x_dates = [ start_dates[i] + ' -> ' + end_dates[i] for i in df_range ]
    # lower_stdev = [ mean[i] - stdev[i] for i in df_range ]
    # upper_stdev = [ mean[i] + stdev[i] for i in df_range ]

    fig, (dtd, totals) = plt.subplots(nrows=2, ncols=1, sharex=True)

    dtd_scatter_x, dtd_scatter_y, dtd_scatter_size = [], [], []
    dtd_plot_x, dtd_plot_y = [], []
    totals_x, totals_y = [], []

    for _, row in _df.iterrows():
        row = dict(row.items())

        # Generate fig 'xtick_date'.
        start_date, end_date = [ i.strftime('%m-%d') for i in [row['StartDate'], row['EndDate']] ]
        xtick_date = start_date + ' to ' + end_date

        # Loop through 'DAYS_COLS' to collect values for days-to-deliver scatter plot.
        for day in range(len(DAYS_COLS)):
            dtd_scatter_x.append(xtick_date)
            dtd_scatter_y.append(day + 1)
            dtd_scatter_size.append(row[DAYS_COLS[day]])

        # Collect values for days-to-deliver plot.
        dtd_plot_x.append(xtick_date)
        dtd_plot_y.append(row['Mean'])

        # Collect values for totals plot.
        totals_x.append(xtick_date)
        totals_y.append(row['TotalShipped'] - row['DaysMaxFreqPlus'])

    # Generate graphs with collected values.
    dtd.scatter(dtd_scatter_x, dtd_scatter_y, s=dtd_scatter_size)
    dtd.plot(dtd_plot_x, dtd_plot_y, c='lightblue')
    totals.plot(totals_x, totals_y, 'o-')

    # Update totals graph with annotations.
    for i in range(len(totals_x)):
        totals.annotate(
            totals_y[i], (totals_x[i], totals_y[i] + 3), textcoords='offset pixels', xytext=(0, 12),
            ha='center', bbox={'boxstyle': 'square', 'fc': 'white'}
        )
    totals.set_ylim(0, totals.set_ylim()[1] * 1.25)

    # Final fig touch ups then render graphs.
    plt.xticks(rotation=30, ha='right')
    dtd.set_ylabel('days to deliver')
    totals.set_ylabel('total packages shipped')
    fig.tight_layout()
    plt.show()



############
main()   ###
############
