
################################################
"""   <><><>   UNDER CONSTRUCTION   <><><>   """
################################################

import pandas
import matplotlib.pyplot as plt
from datetime import datetime
from collections import OrderedDict
from Required import Connections

begin = datetime.now()

conn = Connections.connect()
cur = conn.cursor()

VALUES = {
    'company_id': 1899,
    'shipped_method': 'UPS MI BPM',
    'max_freq': 14,
    'date_range_type': 'week',
    'greater_date': '2019-10-06',
    'less_date': '2019-10-27'
}

COLUMNS = ['CompanyID', 'StartDate', 'EndDate', 'TotalShipped', 'DaysMaxFreqPlus']
COLUMNS = COLUMNS + [ 'Days' + str(i + 1) for i in range(VALUES['max_freq']) ]

####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    stats = getStatsByGroup(VALUES)

    stats_df = convertStatsToDf(stats)
    print(stats_df)

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



############
main()   ###
############
