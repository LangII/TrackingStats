


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

""" COMMENT IN (and input values) for single manual input ...
    -   ['start_date'] should be empty for automated data pull.  If ['start_date'] is empty then
        TOTALS report will accumulate as well as individuals.  If ['start_date'] is not empty, then
        TOTALS report will not accumulate and be ignored. """
# SERIES = [{
#     'company_id':       1899,
#     'shipped_method':   'UPS MI Parcel Select',
#     'date_range_type':  'week',
#     'max_freq':         14,
#     # 'start_date':       ''
#     'start_date':       '2019-10-20'
# }]

CSV_PATH = 'prints/tests/'

COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE, MAX_FREQ, START_DATE = 0, '', '', 0, ''

# Also controls the order by which the carriers are displayed in 'overview_tab'.
# CARRIERS = ['UPS', 'USPS', 'DHL', 'FedEx']
CARRIERS = ['USPS', 'UPS', 'DHL', 'FedEx']

COLUMNS = ['CompanyID', 'ShippedMethod', 'StartDate', 'EndDate', 'TotalShipped', 'DaysMaxFreqPlus']
STATS_COLS = ['Mean', 'StDev']
TOTALS_COLS = [
    'StartDate', 'EndDate', 'Carrier', 'ShippedMethod', 'CompanyID', 'TotalShipped', 'Mean',
    'StDev', 'DaysMaxFreqPlus'
]

PERFORM_CSV_SAVES = False
ONE_OFF_CSV_NAME_SUFFIX = ''

CSV_NAMES = {'singles': [], 'totals': ''}

TOTALS_PREFIX_HEADERS = ['week of', 'labels']
TOTALS_VALUES = ['TotalShipped', 'Mean', 'StDev', 'DaysMaxFreqPlus']
UPDATED_TOTALS_VALUES = ['total packages', 'average dtd', 'deviation', 'not delivered (<14days)']



####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    global COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE, MAX_FREQ, START_DATE, DAYS_COLS, COLUMNS

    collected_dfs = []
    for index, set in enumerate(SERIES):

        # Get constants per 'set' in SERIES.
        COMPANY_ID =        set['company_id']
        SHIPPED_METHOD =    set['shipped_method']
        DATE_RANGE_TYPE =   set['date_range_type']
        MAX_FREQ =          set['max_freq']

        # Generate START_DATE from current datetime if ['start_date'] is not given.
        if not set['start_date']:   START_DATE, MULTIPLE_DATES = getStartDate(), False
        else:                       START_DATE, MULTIPLE_DATES = set['start_date'], True
        # Update COLUMNS with 'Days' columns generated with MAX_FREQ.
        DAYS_COLS = [ 'Days' + str(i + 1) for i in range(MAX_FREQ - 1) ]
        COLUMNS += DAYS_COLS

        print("\n\n\n>>> current set ({}) in SERIES ({}):".format(index + 1, len(SERIES)))
        print(">>>    CompanyID     =", COMPANY_ID)
        print(">>>    ShippedMethod =", SHIPPED_METHOD)
        print(">>>    DateRangeType =", DATE_RANGE_TYPE)
        print(">>>    MaxFreq       =", MAX_FREQ)
        print(">>>    StartDate     =", START_DATE)

        print("\n>>> getting statistics")
        stats = getStatistics()

        print("\n>>> converting statistics to single dataframe")
        single_df = convertStatsToDf(stats)

        print("\n>>> updating single dataframe with mean and standard deviation")
        single_df = updateDfWithMeanAndStDev(single_df)

        print("\n>>> saving single dataframe to csv")
        saveDfToCsv(single_df, type='single')

        print("\n>>> 'single_df' print out ...\n")
        print(single_df)

        if not MULTIPLE_DATES:
            print("\n>>> adding single dataframe to collection")
            collected_dfs += [single_df]

    print("\n\n\n>>> FINISHED collecting single dataframes")

    if not MULTIPLE_DATES:

        print("\n\n\n>>> concatenating collected dataframes")
        collection_df = pandas.concat(collected_dfs, ignore_index=True)

        print("\n>>> generating totals dataframe (total per shipped method)")
        totals_df_ = generatingTotalsDf(collection_df)

        # FutureWarning: Sorting because non-concatenation axis is not alligned. A future version
        # of pandas will change to not sort by default. To accept the future behavior, pass
        # 'sort=False'. To retain the current behavior and silence the warning, pass 'sort=True'.
        print("\n>>> combining totals df with collected single dataframes")
        totals_df = pandas.concat([collection_df, totals_df_], ignore_index=True, sort=True)

        print("\n>>> dropping and reordering columns for combined dataframe")
        totals_df = totals_df[TOTALS_COLS]

        print("\n>>> 'totals_df_' print out ...\n")
        print(totals_df)

        print("\n>>> saving totals dataframe to csv")
        saveDfToCsv(totals_df, type='totals')

    print("\n>>> generating final totals dataframe")
    final_totals_df = generateFinalTotalsDf()

    testStyling(final_totals_df)

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



def getCsvName(type='single'):
    """
    input:  constants = SHIPPED_METHOD, COMPANY_ID, DATE_RANGE_TYPE, ONE_OFF_CSV_NAME_SUFFIX,
                        MAX_FREQ, CSV_NAMES
    output: Return string 'csv_name_', name given to csv file derived from constants of set of
            SERIES.
    """

    suffix = 'DONTTOUCH' if not ONE_OFF_CSV_NAME_SUFFIX else ONE_OFF_CSV_NAME_SUFFIX

    if type == 'single':
        shipped_method = ''.join([ word.lower().title() for word in SHIPPED_METHOD.split() ])
        name_parts = [str(COMPANY_ID), shipped_method, DATE_RANGE_TYPE + 'ly', suffix]
        csv_name_ = '_'.join(name_parts) + '.csv'
        CSV_NAMES['singles'] += [csv_name_]
    elif type == 'totals':
        csv_name_ = '_'.join(['TOTALS', DATE_RANGE_TYPE + 'ly', suffix]) + '.csv'
        CSV_NAMES['totals'] = csv_name_

    return csv_name_



def saveDfToCsv(_df, type='single'):
    """
    input:  constants = CSV_PATH, CSV_NAME, PERFORM_CSV_SAVES
            _df = Dataframe of set stats with mean and stdev.
    output: Save '_df' to csv designated by input.
    """

    csv_loc = CSV_PATH + getCsvName(type)
    if path.exists(csv_loc):    first_save = False
    else:                       first_save = True

    if PERFORM_CSV_SAVES:   _df.to_csv(csv_loc, mode='a', index=False, header=first_save)
    else:                   print("\n>>> TESTING ...  not performing csv saves")



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



def generatingTotalsDf(_df):
    """
    input:  constants = CARRIERS, STATS_COLS
            _df = Dataframe concatenated from collected set dataframes from SERIES.
    output: Return 'totals_df__', a dataframe of generated totals per 'ShippedMethod'.
    """

    def addCarrier(row):
        """ Subroutine ... Get 'Carrier' from comparing 'ShippedMethod' with 'CARRIERS'. """
        for carrier in CARRIERS:
            if row['ShippedMethod'].startswith(carrier):    return carrier
    _df['Carrier'] = _df.apply(addCarrier, axis='columns')

    # BLOCK ...  Manual sort columns, then rows with 'sort_values'.
    cols = _df.columns.tolist()
    df = _df[cols[:1] + cols[-1:] + cols[1:-1]]
    df = df.sort_values(by='ShippedMethod')
    # Sort rows categorically by CARRIERS.
    df['Carrier'] = pandas.Categorical(df['Carrier'], CARRIERS)
    df = df.sort_values('Carrier')

    # Prepwork for 'totals' rows.
    copy_cols = ['Carrier', 'ShippedMethod', 'StartDate', 'EndDate']
    not_sum_cols = copy_cols + STATS_COLS + ['CompanyID']
    sum_cols = [ col for col in df.columns.tolist() if col not in not_sum_cols ]

    # Create and insert 'totals' (per 'ShippedMethod' (sm)) rows.
    totals_df_s = []
    for sm in df['ShippedMethod'].unique().tolist():

        # Get sub df of individual shipped methods.  Then create working totals rows by copying
        # first row of sub df.
        sm_sub_df = df.loc[df['ShippedMethod'] == sm]
        sm_totals_df_ = sm_sub_df.iloc[[0]].copy()

        # Reset unwanted values of totals row and reset 'CompanyID'.
        for col in sm_totals_df_.columns.tolist():
            if col not in copy_cols:  sm_totals_df_[col] = 0
        sm_totals_df_['CompanyID'] = 'TOTALS'

        # BLOCK ...  Generate 'totals' values.  First by summing values from 'sum_cols', then drop
        # 'Mean' and 'StDev' (STATS_COLS) before generating them with updateDFWithMeanAndStDev().
        for col in sum_cols:  sm_totals_df_[col] = sum(sm_sub_df[col].tolist())
        sm_totals_df_ = sm_totals_df_.drop(STATS_COLS, axis='columns')
        sm_totals_df_ = updateDfWithMeanAndStDev(sm_totals_df_)

        totals_df_s += [sm_totals_df_]

    totals_df__ = pandas.concat(totals_df_s, ignore_index=True)

    return totals_df__



def getCompanyName(_id):

    query = "SELECT CompanyName FROM tblCompany WHERE CompanyID = {}".format(_id)
    cur.execute(query)
    select_ = cur.fetchone()[0]

    return select_



def generateFinalTotalsDf():
    """
    input:
    output:
    """

    # Get accumulated totals dataframe.
    raw_df = pandas.read_csv(CSV_PATH + CSV_NAMES['totals'], encoding='ISO-8859-1')

    # Add zero padding to 'CompanyID' for consistent sorting.
    def zeroPadding(row):
        return row['CompanyID'].rjust(4, '0') if row['CompanyID'] != 'TOTALS' else row['CompanyID']
    raw_df['CompanyID'] = raw_df.apply(zeroPadding, axis='columns')

    # Sort 'raw_df' with parallel arrays 'sort_by' and 'sort_asc'.
    sort_by, sort_asc = ['StartDate', 'ShippedMethod', 'CompanyID'], [False, True, True]
    raw_df = raw_df.sort_values(by=sort_by, ascending=sort_asc)

    # BLOCK ...  Convert columns 'StartDate' and 'EndDate' to 'week of'.
    def modifyDateCols(row):
        def modifyDateString(_date):
            date = _date[_date.find('-') + 1:]
            # Remove padded zeros.
            month, day = [ i[1:] if int(i) <= 9 else i for i in [date[:2], date[3:]] ]
            return month + '/' + day
        return modifyDateString(row['StartDate']) + ' to ' + modifyDateString(row['EndDate'])
    raw_df['week of'] = raw_df.apply(modifyDateCols, axis='columns')

    # Rearrange (and drop) columns.
    raw_df = raw_df.drop(['StartDate', 'EndDate'], axis='columns')
    cols = raw_df.columns.tolist()
    raw_df = raw_df[cols[-1:] + cols[:-1]]

    print(raw_df)

    """ Start build of 'totals_df_'. """

    # Get list of dates.
    dates = raw_df['week of'].unique().tolist()

    # Get dataframe of first date group for getting headers.
    first_date_df = raw_df.loc[raw_df['week of'] == dates[0]]

    # Initiate 'totals_df_' with generic 'totals_cols' columns.
    totals_cols = (first_date_df['ShippedMethod'] + ' - ' + first_date_df['CompanyID']).tolist()
    totals_df_ = pandas.DataFrame(columns=TOTALS_PREFIX_HEADERS + totals_cols)

    # Build 'totals_df_' with rows of headers.
    for header_row in ['Carrier', 'ShippedMethod', 'CompanyID']:
        first_two = ['', ''] if header_row != 'CompanyID' else TOTALS_PREFIX_HEADERS
        totals_df_.loc[len(totals_df_)] = first_two + first_date_df[header_row].tolist()

    # BLOCK ...  Populating table with data.
    # Handle each date block.
    for date in dates:
        # Get sub-dataframe to parse out data per date block.
        single_date_df = raw_df.loc[raw_df['week of'] == date]
        # Each row within each 'date', represents a 'value' from TOTALS_VALUES.
        for value in TOTALS_VALUES:
            # Append collected values to 'totals_df_'.
            totals_df_.loc[len(totals_df_)] = [date, value] + single_date_df[value].tolist()

    # Replace 'CompanyID' values with associated 'CompanyName'.
    companies = totals_df_.iloc[2].tolist()
    company_names = [ getCompanyName(int(c)) if c.isdigit() else c for c in companies ]
    totals_cols = totals_df_.columns.tolist()
    for i, name in enumerate(company_names):  totals_df_.at[2, totals_cols[i]] = name

    # Replace 'labels' with more readable 'labels'.
    labels, new_labels = totals_df_['labels'].tolist(), []
    for l in labels:
        new_labels += [UPDATED_TOTALS_VALUES[TOTALS_VALUES.index(l)] if l in TOTALS_VALUES else l]
    for i, label in enumerate(new_labels):  totals_df_.at[i, 'labels'] = label

    totals_df_.to_csv(CSV_PATH + 'temp.csv', index=False)

    return totals_df_



def testStyling(_test_df):

    print("\n\n\n>>>   <><><> TESTING <><><>\n")

    print(_test_df)

    # Build 'sheet' for excel styling (using 'XlsxWriter' as engine).
    writer = pandas.ExcelWriter(CSV_PATH + 'teststyle01.xlsx', engine='xlsxwriter')
    _test_df.to_excel(writer, sheet_name='teststylesheet01', header=False, index=False)
    book, sheet = writer.book, writer.sheets['teststylesheet01']

    """   Merge label cells.   """

    merge_format = book.add_format({'align': 'center', 'valign': 'vcenter'})

    # Start to build container for looping through merge sets with TOTALS set.
    merges = [[0, 0, 1, 1, 'TOTALS']]

    def getHeaderMerges(_row):

        row_as_list = _test_df.loc[_row].tolist()
        uniques = sorted(list(set(row_as_list)))[1:]

        header_merges_ = []
        for u in uniques:
            start_row, end_row = _row, _row
            start_col = row_as_list.index(u)
            end_col = max([ i for i, x in enumerate(row_as_list) if x == u ])
            header_merges_ += [[start_row, start_col, end_row, end_col, u]]

        return header_merges_

    # User 'getHeaderMerges()' to get merge arguments from header rows.
    for i in [0, 1]:  merges += getHeaderMerges(i)

    # Get merge sets for 'week of' column (dates) and append to 'merges'.
    dates_list = _test_df['week of'].tolist()
    unique_dates = sorted(list(set(dates_list)))[1:-1]
    for u in unique_dates:
        start_col, end_col = 0, 0
        start_row = dates_list.index(u)
        end_row = max([ i for i, x in enumerate(dates_list) if x == u ])
        merges += [[start_row, start_col, end_row,end_col, u]]

    # Loop through 'merges' to perform individual merges on 'sheet'.
    for start_row, start_col, end_row, end_col, value in merges:
        sheet.merge_range(start_row, start_col, end_row, end_col, value, merge_format)

    """   Conditional formatting.   """

    # all = book.add_format({'font_name': '', 'font_size': ''})
    bordering = book.add_format({'border': 2})
    bolding = book.add_format({'bold': True})
    totals_values = book.add_format({'right': 1, 'bold': True})
    date_block = book.add_format({'bottom': 1})
    corners = book.add_format({'right': 1, 'bottom': 1, 'bold': True})

    df_width, df_height = len(_test_df.columns) - 1, len(_test_df) - 1

    sheet.conditional_format(0, 0, 2, df_width, {'type': 'no_errors', 'format': bordering})
    sheet.conditional_format(0, 0, df_height, 1, {'type': 'no_errors', 'format': bordering})
    sheet.conditional_format(0, 0, 1, df_width, {'type': 'no_errors', 'format': bolding})
    totals_bolding = {'type': 'cell', 'criteria': '==', 'value': '"TOTALS"', 'format': bolding}
    sheet.conditional_format(2, 0, 2, df_width, totals_bolding)

    totals_cols = [ i for i, value in enumerate(_test_df.loc[2].tolist()) if value == 'TOTALS' ]
    date_block_rows = [ i for i, label in enumerate(_test_df['labels'].tolist()) if label == UPDATED_TOTALS_VALUES[-1] ]

    for col in totals_cols:
        for row in date_block_rows:
            sheet.conditional_format(row, col, row, col, {'type': 'no_errors', 'format': corners})

    totals_cols_cons = {'type': 'no_errors', 'format': totals_values}
    for tc in totals_cols:  sheet.conditional_format(3, tc, df_height, tc, totals_cols_cons)

    for dbr in date_block_rows:  sheet.conditional_format(dbr, 2, dbr, df_width, {'type': 'no_errors', 'format': date_block})



    """
    TURNOVER NOTES ...
    In process of styling TOTALS report.
    HAVE DONE:
    - All merges, all borders, and all bolds.
    - Renaming of company ids to company names and renaming of values to easier terminology.
    NEED TO:
    - Of all cells, set font name and font size.
    - Set column width.
    - Set background color of values in average dtd rows.
    - Clean up code.
    """



    sheet.freeze_panes(3, 2)
    writer.save()

    return





############
main()   ###
############
