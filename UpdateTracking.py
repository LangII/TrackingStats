


"""

UpdateTracking.py

- 2019-12-02 by David Lang
    - Added constant RECHECKING_YESTERDAY and conditionals to query of getPackages().  Conditionals are
    to check if package data was already pulled that day.  This is so data is not repeatedly
    retrieved unnecessarily if script is repeatedly run due to time outs or errors.  Then
    RECHECKING_YESTERDAY makes this functionality toggleable for debugging.

- 2019-11-20 by David Lang
    - Update database with tracking event Message, event MessageTimestamp, and event boolean
    Delivered, from designated conditions of CompanyID, ShippedMethod, and (DaysAgo or (StartDate
    and EndDate)).

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

import UpdateTrackingSettings as settings
from datetime import datetime, timedelta
from Required import Connections, Tracking, Mail
import pandas



####################################################################################################
                                                                                 ###   GLOBALS   ###
                                                                                 ###################

begin = datetime.now()

conn = Connections.connect()
cur = conn.cursor()



####################################################################################################
                                                                               ###   CONSTANTS   ###
                                                                               #####################

# 'getPackages()', 'updateTableArrival()'
COMPANY_ID = 0

# 'getPackages()'
SHIPPED_METHOD  = ''
DAYS_AGO        = 0
START_DATE      = ''   # <-- Only used if 'DAYS_AGO = 0'.
END_DATE        = ''   # <-- Only used if 'DAYS_AGO = 0'.

# 'getCarrier()'
CARRIERS = ['UPS', 'USPS', 'DHL', 'FedEx']

""" commented in/out 2020-01-09 (buggy) """
# Toggle 'LastChecked < YESTERDAY' conditional from 'getPackages() query'...  To recheck shipments (or
# not to recheck shipments), that were already checked that day, and have yet to be delivered.
RECHECKING_YESTERDAY = False

YESTERDAY = (begin - timedelta(days=1)).strftime('%Y-%m-%d')

EMAIL_PACKAGE = {'totals': [], 'errors': []}

DAYS_AGO = settings.days_ago
SERIES = settings.series

# DEBUG ... For single series manual input.
# DAYS_AGO = 30
# SERIES = [{'company_id': 507,  'shipped_method': 'USPS Media Mail'}]



####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    global COMPANY_ID, SHIPPED_METHOD, DAYS_AGO, EMAIL_PACKAGE

    for i, set in enumerate(SERIES):
        COMPANY_ID, SHIPPED_METHOD = set['company_id'], set['shipped_method']

        mainLoop(i + 1, len(SERIES))

        print("\n>>> COMPLETED updating tracking ... {}, {}".format(COMPANY_ID, SHIPPED_METHOD))

    print("\n>>> sending recap email ...")
    sendRecapEmail(EMAIL_PACKAGE)

    end = datetime.now()
    exit("\n>>> DONE ... runtime = " + str(end - begin) + "\n\n\n\n")



def mainLoop(_set, _series):

    global EMAIL_PACKAGE

    updateStartAndEndDates()

    print("\n>>> retrieving packages for:")
    print(">>>    CompanyID     =", COMPANY_ID)
    print(">>>    ShippedMethod =", SHIPPED_METHOD)
    print(">>>    StartDate     =", START_DATE)
    print(">>>    EndDate       =", END_DATE)

    # packages = getSinglePackageById(13544607)

    carrier = getCarrier()

    packages = getPackages()
    packages = filterMultiTrackingNums(packages)
    print("\n>>> retrieved", len(packages), "packages")

    set_print = "\n>>> set = {} of {} ... {}, {}".format(_set, _series, COMPANY_ID, SHIPPED_METHOD)

    for i, (package_shipment_id, tracking_number) in enumerate(packages):

        print(set_print)
        print(">>> package =", i + 1, "of", len(packages))
        print(">>> PackageShipmentID =", package_shipment_id, "/ TrackingNumber =", tracking_number)

        if   carrier == 'UPS':      vitals = Tracking.getSingleUpsVitals(tracking_number)
        elif carrier == 'USPS':     vitals = Tracking.getSingleUspsVitals(tracking_number)
        elif carrier == 'DHL':      vitals = Tracking.getSingleDhlVitals(tracking_number)
        elif carrier == 'FedEx':    vitals = Tracking.getSingleFedExVitals(tracking_number)
        if vitals == 'error':
            print(">>>     - BAD RESPONSE ... not updating ... moving on ... \\_(**)_/")
            EMAIL_PACKAGE['errors'] += [[
                COMPANY_ID, SHIPPED_METHOD, package_shipment_id, tracking_number
            ]]
            continue
        print(">>>     - vitals retrieved")

        updateTableArrival(package_shipment_id, tracking_number, vitals)
        print(">>>     - tblArrival updated")

    EMAIL_PACKAGE['totals'] += [[str(COMPANY_ID), SHIPPED_METHOD, str(len(packages))]]



####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

def updateStartAndEndDates():
    """ Update values of START_DATE and END_DATE based on value of DAYS_AGO. """
    global START_DATE, END_DATE
    if DAYS_AGO != 0:
        START_DATE  = (begin - timedelta(days=DAYS_AGO)).strftime('%Y-%m-%d')
        END_DATE    = YESTERDAY



def getCarrier():
    """
    input:  constants = CARRIERS, SHIPPED_METHOD
    output: Return string 'carrier_', a tag to determine which carrier's method to use based on
            input of 'SHIPPED_METHOD'.
    """
    carrier_ = ''

    for c in CARRIERS:
        carrier_in_shipped_method = (SHIPPED_METHOD.lower()).startswith(c.lower())
        if carrier_in_shipped_method:
            carrier_ = c
            break

    if not carrier_:  exit("\n>>> ERROR:  unrecognized carrier in 'SHIPPED_METHOD' ... \\_(**)_/\n")

    return carrier_



def getSinglePackageById(_package_shipment_id):
    """
    input:  _package_shipment_id = Single PackageShipmentID.
    output: Return list-of-tuples 'select_'.  This function is for testing purposes.  Return is
            still in list format as to not break other functionality.
    """

    query = """
        SELECT PackageShipmentID, TrackingNumber FROM tblPackageShipments
            WHERE PackageShipmentID = %s
    """
    cur.execute(query, [_package_shipment_id])
    select_ = [ [str(x), y] for x, y in cur.fetchall() ]

    return select_



def getPackages():
    """
    input:  constants:  COMPANY_ID, SHIPPED_METHOD, DAYS_AGO, START_DATE, END_DATE
    output: Return list-of-tuples 'select_', as package shipment ids and tracking numbers of
            packages under queried criteria.
    """

    query = """
        SELECT ps.PackageShipmentID, ps.TrackingNumber
            FROM tblPackageShipments AS ps LEFT JOIN tblArrival AS a
                ON ps.PackageShipmentID = a.PackageShipmentID
            WHERE ps.CompanyID = %s
                AND ps.ShippedMethod = %s
                AND ps.CompletionDate > %s
                AND ps.CompletionDate < %s
                AND (a.Delivered != 'Y' OR a.Delivered IS NULL)
                {}
    """

    """ commented in/out 2020-01-09 (buggy) """
    if not RECHECKING_YESTERDAY:
        query = query.format('AND (a.LastChecked < %s OR a.LastChecked IS NULL)')
        insert = [COMPANY_ID, SHIPPED_METHOD, START_DATE, END_DATE, YESTERDAY]
    else:
        query = query.format('')
        insert = [COMPANY_ID, SHIPPED_METHOD, START_DATE, END_DATE]

    """ commented in/out 2020-01-09 (buggy) """
    # query = query.format('')
    # insert = [COMPANY_ID, SHIPPED_METHOD, START_DATE, END_DATE]

    cur.execute(query, insert)
    select_ = [ [str(x), y] for x, y in cur.fetchall() ]

    return select_



def filterMultiTrackingNums(_packages):
    """
    Quick filtering for packages with multiple tracking numbers.  Then if found, multiple tracking
    number packages are separated into individual packages with individual tracking numbers.
    """

    def splitTrackingNums(_pack):
        """ Subroutine...  Separate of packages with multiple tracking numbers. """
        multi = [ i.strip() for i in _pack[1].split(';') ]
        splits_ = [ [_pack[0], m] for m in multi ]
        return splits_

    packages = []
    for pack in _packages:
        if ';' in pack[1]:
            for split in splitTrackingNums(pack):  packages.append(split)
        else:
            packages.append(pack)

    return packages



def updateTableArrival(_package_shipment_id, _tracking_number, _vitals):
    """
    input:  constants = COMPANY_ID
            _package_shipment_id = Package shipment ID of package to update.
            _tracking_number = Tracking number of package to update.
            _vitals['delivered'] = Bool of delivery confirmation.
            _vitals['message'] = Description of most recent package event.
            _vitals['time_stamp'] = Time stamp of most recent package event.
    output: Update tblArrival with input values.
    """

    query = """
        INSERT INTO tblArrival (
            PackageShipmentID, TrackingNumber, MessageTimestamp, Message, CompanyID, Delivered
        )
            VALUES (%s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                Message = VALUES(Message),
                LastChecked = NOW(),
                MessageTimestamp = VALUES(MessageTimestamp),
                Delivered = VALUES(Delivered)
    """
    insert = [
        _package_shipment_id,   _tracking_number,
        _vitals['time_stamp'],  _vitals['message'],
        COMPANY_ID,             'Y' if _vitals['delivered'] else 'N'
    ]
    cur.execute(query, insert)
    conn.commit()



def sendRecapEmail(_email_package):

    EMAIL_CSV_NAME = 'errors_email.csv'

    email_to = ['dlang@disk.com']
    email_from = 'dlang@disk.com'
    email_message = ''
    email_subject = 'UpdateTracking.py recap'
    file_loc = EMAIL_CSV_NAME
    file_name = 'errors.csv'

    email_df_cols = ['comp id', 'ship meth', 'package id', 'track num']

    email_message += '\ncomp_id ... ship_meth ... qty\n'
    for total in _email_package['totals']:  email_message += '\n' + ' ... '.join(total)

    # mes_recap_headers = ['comp id', 'ship meth', 'total']
    # padding = [0, 0, 0]
    #
    #
    # for total in [mes_recap_headers] + _email_package['totals']:
    #     for i, t in enumerate(total):
    #         if len(str(t)) > padding[i]:  padding[i] = len(str(t))
    #
    # mes_lines = '\n+-{}-+-{}-+-{}-+'.format(*[ '-' * p for p in padding ])
    # mes_content = '\n| {} | {} | {} |'
    #
    # headers_wpad = []
    # for i, h in enumerate(mes_recap_headers):  headers_wpad += [h.ljust(padding[i])]
    #
    # email_message += '\ntotals of packages tracked per company and shipped method\n'
    # email_message += mes_lines
    # email_message += mes_content.format(*headers_wpad)
    # email_message += mes_lines
    #
    # for pack in _email_package['totals']:
    #     pack_wpad = []
    #     for i, p in enumerate(pack):  pack_wpad += [str(p).ljust(padding[i])]
    #     email_message += mes_content.format(*pack_wpad)
    #
    # email_message += mes_lines

    print(email_message)

    email_df = pandas.DataFrame(_email_package['errors'], columns=email_df_cols)
    email_df['track num'] += '\''
    email_df.to_csv(EMAIL_CSV_NAME, index=False)

    Mail.SendFile(email_to, email_from, email_message, email_subject, file_loc, file_name)



############
main()   ###
############
