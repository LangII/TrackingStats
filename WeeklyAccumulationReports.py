


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
import xlsxwriter



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

CSV_PATH = 'saves/csvs/'
XLSX_PATH = 'saves/'

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
UPDATED_TOTALS_VALUES = ['total packages', 'average dtd', 'deviation', 'not delivered (<2w)']

UPDATED_SINGLES_COL_VALUES = {
    # convert from      # convert to
    'CompanyID':       'CompanyName',
    'TotalShipped':    'TotalPackages',
    'Mean':            'AverageDTD',
    'DaysMaxFreqPlus': 'NotDelivered(<2w)',
    'StDev':           'Deviation',
}

XLSX_SAVE_NAME = 'weekly_accumulation_report.xlsx'
XLSX_EMAIL_NAME = ''

SINGLE_CSVS = []

FORMAT = {}



####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    global COMPANY_ID, SHIPPED_METHOD, DATE_RANGE_TYPE, MAX_FREQ, START_DATE, DAYS_COLS, COLUMNS
    global SINGLE_CSVS

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
        print(">>>    CompanyID =    ", COMPANY_ID)
        print(">>>    ShippedMethod =", SHIPPED_METHOD)
        print(">>>    DateRangeType =", DATE_RANGE_TYPE)
        print(">>>    MaxFreq =      ", MAX_FREQ)
        print(">>>    StartDate =    ", START_DATE)

        print("\n>>> getting statistics")
        stats = getStatistics()

        print("\n>>> converting statistics to single dataframe")
        single_df = convertStatsToDf(stats)

        print("\n>>> updating single dataframe with mean and standard deviation")
        single_df = updateDfWithMeanAndStDev(single_df)

        print("\n>>> saving single dataframe to csv")
        saveDfToCsv(single_df, type='single')
        SINGLE_CSVS += [getCsvName()]

        print("\n>>> 'single_df' print out ...\n")
        print(single_df)

        print("\n>>> adding single dataframe to collection")
        collected_dfs += [single_df]

    print("\n\n\n>>> FINISHED collecting single dataframes")

    print("\n\n\n>>> concatenating collected dataframes")
    collection_df = pandas.concat(collected_dfs, ignore_index=True)

    print("\n>>> generating totals dataframe (total per shipped method)")
    totals_df = generatingTotalsDf(collection_df)

    # "FutureWarning: Sorting because non-concatenation axis is not alligned. A future version
    # of pandas will change to not sort by default. To accept the future behavior, pass
    # 'sort=False'. To retain the current behavior and silence the warning, pass 'sort=True'."
    print("\n>>> combining totals df with collected single dataframes")
    with_totals_df = pandas.concat([collection_df, totals_df], ignore_index=True, sort=True)

    print("\n>>> dropping and reordering columns for combined dataframe")
    with_totals_df = with_totals_df[TOTALS_COLS]

    print("\n>>> saving with totals dataframe to csv")
    saveDfToCsv(with_totals_df, type='totals')

    print("\n>>> generating raw dataframe from data in saved csvs")
    raw_totals_df = generateRawTotalsDfFromCsvs()

    print("\n>>> converting raw dataframe to final totals dataframe")
    final_totals_df = convertRawDfToFinalTotalsDf(raw_totals_df)

    print("\n>>> building xlsx output file")
    book = xlsxwriter.Workbook(XLSX_PATH + XLSX_SAVE_NAME)

    print("\n>>> build global formating dict")
    buildFormat(book)

    print("\n>>> building totals xlsx sheet")
    book = buildTotalsSheet(book, final_totals_df)

    single_csvs_by_tab = getTabOrder(SINGLE_CSVS, final_totals_df.columns.tolist())

    print("\n>>> generate single company tabs in xlsx output")
    for single_csv_name in single_csvs_by_tab:

        print("\n>>> retrieve saved company csv and convert to final dataframe format")
        raw_df = pandas.read_csv(CSV_PATH + single_csv_name, encoding='ISO-8859-1')
        single_df = convertRawDfToFinalSingleDf(raw_df)

        print("\n>>> convert final company dataframe to styled xlsx tab")
        book = generateSingleSheet(book, single_df, single_csv_name)

    print("\n\n\n>>> FINISHED generating single xlsx company tabs")

    book.close()

    run_time = str(datetime.now() - begin)
    exit("\n>>> DONE ... runtime = " + run_time + "\n\n\n")



####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

def getStartDate():
    """ Return 'start_date_', string of datetime object used in sql query. """

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
                AND StartDate = %s
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
    input:  constants = STATS_COLS, MAX_FREQ
            _df = Dataframe from 'convertStatsToDf()'.
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

        # Patch for freak occurence of there only being a single package worth of data.
        if len(one_dim_array) == 1:  one_dim_array += one_dim_array

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
    """ Return 'select_', a string of company name derived from int of company id. """

    query = "SELECT CompanyName FROM tblCompany WHERE CompanyID = {}".format(_id)
    cur.execute(query)
    select_ = cur.fetchone()[0]

    return select_



def modifyDateCols(row):
    """ Return 'week_of_', convert value from 'StartDate' and 'EndDate' into 'week of' value. """

    def modifyDateString(_date):
        date = _date[_date.find('-') + 1:]
        # Remove padded zeros.
        month, day = [ i[1:] if int(i) <= 9 else i for i in [date[:2], date[3:]] ]
        return month + '/' + day

    week_of_ = modifyDateString(row['StartDate']) + ' to ' + modifyDateString(row['EndDate'])

    return week_of_



def generateRawTotalsDfFromCsvs():
    """
    input:  constants = CSV_PATH, CSV_NAMES
    output: Return 'raw_df_', dataframe object of data converted from CSV_NAMES['totals'].  Minor
            adjustments made to table like sorts and drops.
    """

    # Get accumulated totals dataframe.
    raw_df_ = pandas.read_csv(CSV_PATH + CSV_NAMES['totals'], encoding='ISO-8859-1')

    # Add zero padding to 'CompanyID' for consistent sorting.
    def zeroPadding(row):
        return row['CompanyID'].rjust(4, '0') if row['CompanyID'] != 'TOTALS' else row['CompanyID']
    raw_df_['CompanyID'] = raw_df_.apply(zeroPadding, axis='columns')

    # Sort 'raw_df_' with parallel arrays 'sort_by' and 'sort_asc'.
    sort_by, sort_asc = ['StartDate', 'ShippedMethod', 'CompanyID'], [False, True, True]
    raw_df_ = raw_df_.sort_values(by=sort_by, ascending=sort_asc)

    # Apply 'modifyDateCols()' to 'raw_df_'.
    raw_df_['week of'] = raw_df_.apply(modifyDateCols, axis='columns')

    # Rearrange (and drop) columns.
    raw_df_ = raw_df_.drop(['StartDate', 'EndDate'], axis='columns')
    cols = raw_df_.columns.tolist()
    raw_df_ = raw_df_[cols[-1:] + cols[:-1]]

    return raw_df_



def convertRawDfToFinalTotalsDf(_raw_df):
    """
    input:  constants = TOTALS_PREFIX_HEADERS, UPDATED_TOTALS_VALUES, TOTALS_VALUES
            _raw_df = Dataframe object from generateRawTotalsDfFromCsvs().
    output: Return 'totals_df_', dataframe object from '_raw_df' with modifications.
    """

    # Get list of dates.
    dates = _raw_df['week of'].unique().tolist()

    # Get dataframe of first date group for getting headers.
    first_date_df = _raw_df.loc[_raw_df['week of'] == dates[0]]

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
        single_date_df = _raw_df.loc[_raw_df['week of'] == date]
        # Each row within each 'date', represents a 'value' from TOTALS_VALUES.
        for value in TOTALS_VALUES:
            # Append collected values to end of 'totals_df_'.
            totals_df_.loc[len(totals_df_)] = [date, value] + single_date_df[value].tolist()

    # Replace 'CompanyID' values with associated 'CompanyName'.
    companies = totals_df_.iloc[2].tolist()
    company_names = [ getCompanyName(int(c)) if c.isdigit() else c for c in companies ]
    df_cols = totals_df_.columns.tolist()
    for i, name in enumerate(company_names):  totals_df_.at[2, df_cols[i]] = name
    print(name)
    for i, name in enumerate(totals_df_.iloc[2].tolist()):
        if name == 'Michael Hyatt and Company':  totals_df_.at[2, df_cols[i]] = 'Michael Hyatt & Co'
        if name == 'The Healthy Back Institute':  totals_df_.at[2, df_cols[i]] = 'Healthy Back'

    # Replace 'labels' with more readable 'labels'.
    labels, new_labels = totals_df_['labels'].tolist(), []
    for l in labels:
        new_labels += [UPDATED_TOTALS_VALUES[TOTALS_VALUES.index(l)] if l in TOTALS_VALUES else l]
    for i, label in enumerate(new_labels):  totals_df_.at[i, 'labels'] = label

    return totals_df_



def buildFormat(_book):
    """
    input:  constants = FORMAT
            _book = xlsxwriter Workbook object, needed for building formats with 'add_format()'.
    output: Globalized formats added to FORMAT.
    """

    global FORMAT

    # Assign format dicts.
    all_font_size = {'font_size': 12}
    border_bold =   {'border': 2}
    set_num_dec =   {'num_format': '0.00'}
    avg_bgc =       {'bg_color': '#E0E0E0'}
    all_values =    {**all_font_size, **{'align': 'center'}}
    all_labels =    {**all_font_size, **border_bold}

    # Build FORMAT.
    FORMAT = {**FORMAT, **{
        'merge':            _book.add_format({'align': 'center', 'valign': 'vcenter'}),
        'all':              _book.add_format(all_font_size),
        'values':           _book.add_format(all_values),
        'val_right':        _book.add_format({**all_values, **{'right': 1, 'bold': True}}),
        'val_bottom':       _book.add_format({**all_values, **{'bottom': 1}}),
        'val_corner':       _book.add_format({
            **all_values, **{'right': 1, 'bottom': 1, 'bold': True}
        }),
        'avg':              _book.add_format({**all_values, **set_num_dec, **avg_bgc}),
        'avg_corner':       _book.add_format({
            **all_values, **set_num_dec, **avg_bgc, **{'right': 1, 'bold': True}
        }),
        'dev':              _book.add_format({**all_values, **set_num_dec}),
        'dev_corner':       _book.add_format({
            **all_values, **set_num_dec, **{'right': 1, 'bold': True}
        }),
        'col_labels':       _book.add_format({**all_labels, **{'align': 'center', 'bold': True}}),
        'comp_row':         _book.add_format({**all_labels, **{'align': 'center'}}),
        'totals':           _book.add_format({**all_labels, **{'align': 'center', 'bold': True}}),
        'labels_cell':      _book.add_format({**all_font_size, **border_bold}),
        'weekof_col':       _book.add_format({
            **all_labels, **{'align': 'center', 'valign': 'vcenter'}
        }),
        'labels_col':       _book.add_format(all_labels),
        'totals_cell':      _book.add_format({
            **all_labels, **{'align': 'center', 'valign': 'vcenter', 'bold': True}
        }),
        'avg_cell':         _book.add_format({**all_labels, **avg_bgc}),
        'single_labels':    _book.add_format({**all_values, **border_bold}),
        'single_avg_cell':  _book.add_format({**all_values, **border_bold, **{'bold': True}}),
        'single_avg':       _book.add_format({**all_values, **set_num_dec, **{'bold': True}}),
        'single_dev':       _book.add_format({**all_values, **set_num_dec}),
    }}



def buildTotalsSheet(_book, _df):
    """
    input:  constants = UPDATED_TOTALS_VALUES, FORMAT
            _book = Workbook object for writing to final xlsx.
            _df = Dataframe object with data to be written to 'sheet'.
    output: Return '_book', original Workbook object from input '_book' but with Worksheet object
            'sheet' added.
    """

    # _book = xlsxwriter.Workbook(CSV_PATH + 'xlsx_output.xlsx')
    sheet = _book.add_worksheet('TOTALS')

    """   perform merges   """

    # Start to build container for looping through merge sets with TOTALS set.
    merges = [[0, 0, 1, 1, 'TOTALS']]

    def getHeaderMerges(_row):
        """ Subroutine to get 'header_merges_' for building 'merges'. """

        row_as_list = _df.loc[_row].tolist()
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
    dates_list = _df['week of'].tolist()
    unique_dates = sorted(list(set(dates_list)))[1:-1]
    for u in unique_dates:
        start_col, end_col = 0, 0
        start_row = dates_list.index(u)
        end_row = max([ i for i, x in enumerate(dates_list) if x == u ])
        merges += [[start_row, start_col, end_row,end_col, u]]

    # Loop through 'merges' to perform individual merges on 'sheet'.
    for start_row, start_col, end_row, end_col, value in merges:
        sheet.merge_range(start_row, start_col, end_row, end_col, value, FORMAT['merge'])

    """   apply formatting   """

    # Get sizes of '_df'.
    width, height = len(_df.columns), len(_df)

    # Get lists referencing indexes of specific rows and cols (rows with last row of each date
    # block, rows with 'average dtd', and columns with 'TOTALS').
    label_row = _df['labels'].tolist()
    date_end_rows = [ i for i, label in enumerate(label_row) if label == UPDATED_TOTALS_VALUES[-1] ]
    average_rows = [ i for i, label in enumerate(label_row) if label == 'average dtd' ]
    totals_cols = [ i for i, value in enumerate(_df.loc[2].tolist()) if value == 'TOTALS' ]

    # Build 'format_assignments', a list-of-lists, where each sub list is in the format of
    # [list-of-rows, list-of-cols, format applied].  Each row in list-of-rows is compared to each
    # col in list-of-cols, the coordinated cells between rows and cols then receive the applied
    # format.
    format_assignments = [
        # rows                              # cols              # format
        [range(height)[3:],                 range(width)[2:],   'values'],
        [range(height)[3:],                 totals_cols,        'val_right'],
        [date_end_rows,                     range(width)[2:],   'val_bottom'],
        [date_end_rows,                     totals_cols,        'val_corner'],
        [average_rows,                      range(width)[2:],   'avg'],
        [average_rows,                      totals_cols,        'avg_corner'],
        [[ i + 1 for i in average_rows ],   range(width)[2:],   'dev'],
        [[ i + 1 for i in average_rows ],   totals_cols,        'dev_corner'],
        [[0, 1],                            range(width),       'col_labels'],
        [[2],                               range(width),       'comp_row'],
        [[2],                               totals_cols,        'totals'],
        [range(height)[3:],                 [0],                'weekof_col'],
        [range(height)[3:],                 [1],                'labels_col'],
        [average_rows,                      [1],                'avg_cell'],
        [[2],                               [1],                'labels_cell'],
    ]

    # Initial build of 'format_matrix'.
    format_matrix = [ [ FORMAT['all'] for w in range(width) ] for h in range(height) ]

    # Update 'format_matrix' with each detailed assignment from 'format_assignments'.
    for assign_row, assign_col, assign_format in format_assignments:
        for row in assign_row:
            for col in assign_col:
                format_matrix[row][col] = FORMAT[assign_format]

    # Application of values from '_df' and formatting from 'format_matrix' to 'sheet'.
    for row in range(height):
        row_list = _df.loc[row].tolist()
        for col in range(width):
            sheet.write(row, col, row_list[col], format_matrix[row][col])

    # label 'TOTALS' cell value does not come from '_df', i.e. value is inserted manually.
    sheet.write(0, 0, 'TOTALS', FORMAT['totals_cell'])

    # Column width adjustments, then apply freeze panes.
    weekof_colw =   16
    labels_colw =   20
    comps_colw =    20
    totals_colw =   10
    sheet.set_column(0, 0, weekof_colw)
    sheet.set_column(1, 1, labels_colw)
    for col in range(width)[2:]:  sheet.set_column(col, col, comps_colw)
    for col in totals_cols:  sheet.set_column(col, col, totals_colw)
    sheet.freeze_panes(3, 2)

    return _book



def getSingleTabName(_single_csv):
    """ Return 'tab_name_', string derived from name of associated csv.  """

    uscore1 = _single_csv.find('_')
    company_name = getCompanyName(int(_single_csv[:uscore1]))
    uscore2 = _single_csv[uscore1 + 1:].find('_')
    shipped_method = _single_csv[ uscore1 + 1 : uscore1 + uscore2 + 1 ]

    if company_name == 'Michael Hyatt and Company':  company_name = 'MichaelHyatt&Co'
    elif company_name == 'The Healthy Back Institute':  company_name = 'HealthyBack'
    if shipped_method == 'DhlSmartmailParcelExpedited':  shipped_method = 'DHLExpedited'
    elif shipped_method == 'DhlSmartmailParcelPlusExpedited':  shipped_method = 'DHLPlusExpedited'

    # Format 'tab_name_' from 'company_name' and 'shipped_method', then trim if too long.
    tab_name_ = '{}-{}'.format(company_name, shipped_method).replace(' ', '')
    if len(tab_name_) > 31:  tab_name_ = tab_name_[:28] + '...'

    return tab_name_



def getTabOrder(_csvs, _df_cols):
    """
    input:  _csvs = List of strings, each represents a saved csv name.
            _df_cols = List of strings, each represents a column from 'totals_df'.
    output: Return 'tab_order_', list of '_csvs' but ordered by '_df_cols'.
    """

    tab_order_ = []
    for col in _df_cols:
        # Ignore cols with numbers in them.
        if all([ not char.isdigit() for char in col ]):  continue

        # Get and modify 'ship_meth' and 'comp_id' from 'col'.
        ship_meth, comp_id = [ each.strip() for each in col.split('-') ]
        ship_meth = ''.join([ word.lower().title() for word in ship_meth.split() ])
        comp_id = str(int(comp_id))

        # Pull 'csv' from '_csvs' by comparison to 'comp_id' and 'ship_meth'.
        for csv in _csvs:
            if csv.startswith(comp_id + '_' + ship_meth):  tab_order_ += [csv]

    return tab_order_



def convertRawDfToFinalSingleDf(_raw_df):
    """
    input:  constants = UPDATED_SINGLES_COL_VALUES
            _raw_df = Dataframe object direct from single csv save file.
    output: Return 'final_single_df_', data from '_raw_df' gets reordered, updated, dropped, etc as
            final output.
    """

    # Reorder by date descending.
    _raw_df = _raw_df.sort_values(by='StartDate', ascending=False).reset_index(drop=True)

    # Get 'company_name' and 'shipped_method' before columns are dropped.
    company_name = getCompanyName(_raw_df.at[0, 'CompanyID'])
    shipped_method = _raw_df.at[0, 'ShippedMethod']
    # Convert 'StartDate' and 'EndDate' to 'WeekOf'.
    _raw_df['WeekOf'] = _raw_df.apply(modifyDateCols, axis='columns')

    # Drop, rearrange, and rename columns.
    _raw_df = _raw_df.drop(['StartDate', 'EndDate'], axis='columns')
    cols = _raw_df.columns.tolist()
    _raw_df = _raw_df[[cols[-1]] + [cols[2]] + cols[-3:-1] + cols[4:-3] + [cols[3]]]
    _raw_df = _raw_df.rename(columns=UPDATED_SINGLES_COL_VALUES)

    # Insert title header and column headers as first two rows.
    for i, v in enumerate(['col', company_name + ' - ' + shipped_method]):
        cols = { col: v if v != 'col' else col for col in _raw_df.columns.tolist() }
        _raw_df = _raw_df.append(pandas.DataFrame(cols, index=[-i - 1]), ignore_index=False)
    final_single_df_ = _raw_df.sort_index().reset_index(drop=True)

    return final_single_df_



def generateSingleSheet(_book, _df, _csv_name):
    """
    input:  constants = UPDATED_SINGLES_COL_VALUES, FORMAT
            _book = Workbook object for writing to final xlsx.
            _df = Dataframe object with data to be written to 'sheet'.
            _csv_name = String representing csv name, used to get tab name for single sheet.
    output: Return '_book', original Workbook object from input '_book' but with Worksheet object
            'sheet' added.
    """

    sheet = _book.add_worksheet(getSingleTabName(_csv_name))

    # Get '_df' priority indexing.
    cols = _df.columns.tolist()
    width, height = len(cols), len(_df)
    avg_col = cols.index(UPDATED_SINGLES_COL_VALUES['Mean'])
    dev_col = cols.index(UPDATED_SINGLES_COL_VALUES['StDev'])

    # Single merge.
    sheet.merge_range(0, 0, 0, width - 1, _df.at[0, 'WeekOf'], FORMAT['merge'])

    format_assignments = [
        # row               # col           # format
        [[0],               range(width),   'col_labels'],
        [[1],               range(width),   'single_labels'],
        [[1],               [avg_col],      'single_avg_cell'],
        [range(height)[2:], [0],            'single_labels'],
        [range(height)[2:], [avg_col],      'single_avg'],
        [range(height)[2:], [dev_col],      'single_dev'],
    ]

    # Initial build of 'format_matrix'.
    format_matrix = [ [ FORMAT['values'] for w in range(width) ] for h in range(height) ]

    # Update 'format_matrix' with each detailed assignment from 'format_assignments'.
    for assign_row, assign_col, assign_format in format_assignments:
        for row in assign_row:
            for col in assign_col:
                format_matrix[row][col] = FORMAT[assign_format]
    # Application of values from '_df' and formatting from 'format_matrix' to 'sheet'.
    for row in range(height):
        row_list = _df.loc[row].tolist()
        for col in range(width):
            sheet.write(row, col, row_list[col], format_matrix[row][col])

    # Update to column widths, then apply freeze panes..
    col_width_assignments = [
        # cols                  # width
        [[0],                   18],    # WeekOf
        [[1, 2, 3],             14],    # TotalPackages, AverageDTD, Deviation
        [range(width)[4:-1],    8],     # Days
        [[width - 1],           18],    # NotDelivered
    ]
    for cols, width in col_width_assignments:
        for col in cols:  sheet.set_column(col, col, width)
    sheet.freeze_panes(2, 0)

    return _book



############
main()   ###
############
