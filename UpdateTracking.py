
"""

UpdateTracking.py

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

# # FOR EXPORTING
# import sys
# sys.path.insert(0, '/tomcat/python')

import json
import Tracking
from datetime import datetime, timedelta
from Required import Connections



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
COMPANY_ID = 735

# 'getPackages()'
SHIPPED_SERVICE = 'USPS'
SHIPPED_METHOD  = 'USPS Media Mail'
DAYS_AGO        = 30
START_DATE      = '2019-10-01'   # <-- Only used if 'DAYS_AGO = 0'.
END_DATE        = '2019-11-01'   # <-- Only used if 'DAYS_AGO = 0'.

if DAYS_AGO != 0:
    START_DATE = (begin - timedelta(days=DAYS_AGO)).strftime('%Y-%m-%d')
    END_DATE   = begin.strftime('%Y-%m-%d')



####################################################################################################
                                                                                    ###   MAIN   ###
                                                                                    ################

def main():

    print("\n\n>>> retrieving packages for:")
    print(">>>    CompanyID     =", COMPANY_ID)
    print(">>>    ShippedMethod =", SHIPPED_METHOD)
    print(">>>    StartDate     =", START_DATE)
    print(">>>    EndDate       =", END_DATE)

    # packages = getSinglePackageById(13544607)

    packages = getPackages()
    packages = filterMultiTrackingNums(packages)
    print("\n>>> retrieved", len(packages), "packages")

    for i, (package_shipment_id, tracking_number) in enumerate(packages):

        print("\n>>>", i + 1, "of", len(packages))
        print(">>> PackageShipmentID =", package_shipment_id, "/ TrackingNumber =", tracking_number)

        if tracking_number == '':
            print(">>>     - BAD TRACKING NUMBER ... moving on")
            continue

        if   SHIPPED_SERVICE == 'UPS':    vitals = Tracking.getSingleUpsVitals(tracking_number)
        elif SHIPPED_SERVICE == 'USPS':   vitals = Tracking.getSingleUspsVitals(tracking_number)
        elif SHIPPED_SERVICE == 'FedEx':  vitals = Tracking.getSingleFedExVitals(tracking_number)

        print(">>>     -", SHIPPED_SERVICE, "vitals retrieved")

        updateTableArrival(package_shipment_id, tracking_number, vitals)
        print(">>>     - tblArrival updated")

    end = datetime.now()
    exit("\n>>> DONE ... runtime = " + str(end - begin) + "\n\n\n\n")



####################################################################################################
                                                                               ###   FUNCTIONS   ###
                                                                               #####################

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
    """
    insert = [COMPANY_ID, SHIPPED_METHOD, START_DATE, END_DATE]
    cur.execute(query, insert)
    select_ = [ [str(x), y] for x, y in cur.fetchall() ]

    return select_



def filterMultiTrackingNums(_packages):
    """
    Quick filtering for packages with multiple tracking numbers.  Then if found, multiple tracking
    number packages are separated into individual packages with individual tracking numbers.
    """

    def splitTrackingNums(_pack):
        # Subroutine...  Separate of packages with multiple tracking numbers.
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



############
main()   ###
############
