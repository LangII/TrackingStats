


"""

DtdWeeklyPlotSingle.py

- 2019-12-03 by David Lang
    - With entries from tblDaysToDeliverStats generate and display data plots for observing trends
    in days-to-deliver averages.

- turnover notes ...
    - update scatter plot legend labelspacing to be scalable / softcoded
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

COMPANY_ID      = 735
SHIPPED_METHOD  = 'USPS Media Mail'
DATE_RANGE_TYPE = 'week'
GT_ET_DATE      = '2019-10-06'
LT_ET_DATE      = '2019-12-29'
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

    df.to_csv('prints/print_out.csv', index=False)

    generatePlots(df)

    end = datetime.now()
    exit("\n>>> DONE ... runtime = " + str(end - begin) + "\n\n\n")



####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

def getStats():
    """
    input:  constants = COLUMNS, COMPANY_ID, SHIPPED_METHOD, MAX_FREQ, DATE_RANGE_TYPE, GT_ET_DATE,
                        LT_ET_DATE
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
                AND StartDate <= %s
            ORDER BY StartDate ASC
    """
    query = query.format(', '.join(COLUMNS))
    values = [COMPANY_ID, SHIPPED_METHOD, MAX_FREQ, DATE_RANGE_TYPE, GT_ET_DATE, LT_ET_DATE]

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

    new_columns = _df.columns.tolist() + ['Mean', 'StDev']
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



def generatePlots(_df):
    """
    input:  constants = DAYS_COLS, COMPANY_ID, SHIPPED_METHOD
            _df = Dataframe from 'updateDfWithMeandAndStDev()'.
    output: Display plots generated with matplotlib.
    """

    # Initiate local variables.
    fig, (dtd, packages) = plt.subplots(nrows=2, ncols=1, figsize=(10, 8), sharex=True)
    dtd_scatter_x, dtd_scatter_y, dtd_scatter_size = [], [], []
    dtd_plot_x, dtd_plot_m_y, dtd_plot_sdmax_y, dtd_plot_sdmin_y = [], [], [], []
    packages_x, packages_totals_y, packages_nd_y = [], [], []

    for _, row in _df.iterrows():
        row = dict(row.items())

        # Generate 'x_date' for all plots.
        x_date = row['StartDate'].strftime('%m-%d') + ' to ' + row['EndDate'].strftime('%m-%d')

        # Loop through 'DAYS_COLS' to collect values for days-to-deliver scatter plot.
        for day in range(len(DAYS_COLS)):
            dtd_scatter_x.append(x_date)
            dtd_scatter_y.append(day + 1)
            dtd_scatter_size.append(row[DAYS_COLS[day]])

        # Collect values for days-to-deliver plot.
        dtd_plot_x.append(x_date)
        dtd_plot_m_y.append(row['Mean'])
        dtd_plot_sdmax_y.append(row['Mean'] + row['StDev'])
        dtd_plot_sdmin_y.append(row['Mean'] - row['StDev'])

        # Collect values for packages plot.
        packages_x.append(x_date)
        packages_totals_y.append(row['TotalShipped'] - row['DaysMaxFreqPlus'])
        packages_nd_y.append(row['DaysMaxFreqPlus'])

    # Generate dtd plot with collected values.
    dtd_scatter = dtd.scatter(dtd_scatter_x, dtd_scatter_y, s=dtd_scatter_size, c='teal')
    dtd.plot(dtd_plot_x, dtd_plot_sdmax_y, c='lightblue')
    dtd.plot(dtd_plot_x, dtd_plot_m_y, c='orange', label='average')
    dtd.plot(dtd_plot_x, dtd_plot_sdmin_y, c='lightblue', label='deviation')
    dtd.fill_between(dtd_plot_x, dtd_plot_sdmax_y, dtd_plot_sdmin_y, color='lightblue', alpha=0.2)

    # Generate packages plot with collected values.
    packages.plot(packages_x, packages_totals_y, 'o-', c='teal', label='total delivered')
    packages.fill_between(packages_x, packages_totals_y, color='teal', alpha=0.5)
    packages.plot(packages_x, packages_nd_y, 'o-', c='brown', label='not delivered')

    # Update packages plot with annotations.
    for i in range(len(packages_x)):
        packages.annotate(
            packages_totals_y[i], (packages_x[i], packages_totals_y[i] + 3),
            textcoords='offset pixels', xytext=(6, 12), ha='left',
            bbox={'boxstyle': 'square', 'fc': 'white', 'ec': 'teal'}
        )
        packages.annotate(
            packages_nd_y[i], (packages_x[i], packages_nd_y[i] + 3),
            textcoords='offset pixels', xytext=(-6, 12), ha='right',
            bbox={'boxstyle': 'square', 'fc': 'white', 'ec': 'brown'}
        )
    packages.set_ylim(0, packages.set_ylim()[1] * 1.2)

    # Generate scatter plot size legend.
    handles, labels = dtd_scatter.legend_elements(prop='sizes', alpha=1, color='teal')
    size_legend = dtd.legend(
        handles,                    labels,
        loc='upper left',           title="# of packages",
        bbox_to_anchor=(1, 0.8),    ncol=2,
        labelspacing=1.8
    )

    # Final adjustments to 'dtd' plot.
    dtd.set_ylabel('# of days to deliver')
    dtd.legend(loc='lower left', bbox_to_anchor=(1, 0.8)), dtd.add_artist(size_legend)

    # Final adjustments to 'packages' plot.
    packages.set_ylabel('# of packages')
    packages.set_xlabel('date range (by week)')
    packages.legend(loc='lower left', bbox_to_anchor=(1, 0.8))

    # Final adjustments to 'fig' and 'plt'.
    plt.xticks(rotation=30, ha='right')
    title = 'Days to Deliver Details\n{} / {}'.format(COMPANY_ID, SHIPPED_METHOD)
    fig.suptitle(title, size=16)
    fig.tight_layout()
    fig.subplots_adjust(top=0.9)
    plt.show()



############
main()   ###
############
