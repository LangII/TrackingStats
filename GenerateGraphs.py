
################################################
"""   <><><>   UNDER CONSTRUCTION   <><><>   """
################################################

import pandas
import statistics
import matplotlib.pyplot as plt
from datetime import datetime
from collections import OrderedDict
from Required import Connections

begin = datetime.now()

conn = Connections.connect()
cur = conn.cursor()

GROUPS = [
    [1899, 'UPS MI BPM'],       [507 , 'USPS Media Mail'],
    [1603, 'USPS Media Mail'],  [735 , 'USPS Media Mail']
]

VALUES = {
    'company_id': 1899,
    'shipped_method': 'UPS MI BPM',
    'max_freq': 14,
    'date_range_type': 'week',
    'greater_date': '2019-10-06',
    'less_date': '2019-10-27'
}

COLUMNS = ['CompanyID', 'StartDate', 'EndDate', 'TotalShipped', 'DaysMaxFreqPlus']
COLUMNS = COLUMNS + [ 'Days' + str(i + 1) for i in range(VALUES['max_freq']) ][:-1]

####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    stats = {}
    for company_id, shipped_method in GROUPS:
        VALUES['company_id'], VALUES['shipped_method'] = company_id, shipped_method
        group_stats = getStatsByGroup(VALUES)
        stats[company_id] = convertStatsToDf(group_stats)

    # for group in stats:  print(stats[group])

    df = updateDfWithMeanAndStDev(stats[1899])

    end = datetime.now()
    exit("\n>>> DONE ... runtime = " + str(end - begin) + "\n\n\n")

####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

def getStatsByGroup(_values):

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
    # generated twice per dataframe row.

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





############
main()   ###
############
