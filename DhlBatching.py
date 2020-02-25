
"""

This is sort of legacy.  I wrote it within just a couple months of being here...
Proceed with caution.
- David

"""



import TrackingCredentials as cred
import Tracking
import requests
from Required import Connections

DHL_CLIENT_ID = cred.DHL_CLIENT_ID

conn = Connections.connect()
cur = conn.cursor()

testing = False



def getDhlData(batch):
    """
    input:  batch =     Dictionary of entries from 'disk_data' dictionary.  Dhl website only allows 10 entries to be
                        submitted at a time.
    output: Return 'dhl_data' as list of dictionaries compiled from dhl website.  Data collected from website includes
            'time_stamp', 'message', 'tracking_num', and 'shipping_id'.  Each dictionary entry (of collected data)
            corresponds to each entry from the 'batch' parameter.
    """
    # Initiate while-loop conditional variable.
    getDhlData_attempts = 0
    # while-loop used to track and limit number of attempts made to connect to dhl website.
    while getDhlData_attempts < 10:
        # Bool variable used to determine direction after while loop (whether connection was success or not).
        success = False

        try:
            # Get 'tracking' keys from parameter dict 'batch'.
            tracking = list(dict.keys(batch))
            # Build output parameters for retrieving dhl data.
            parameters = {'access_token': Tracking.getDhlKey(), 'client_id': DHL_CLIENT_ID, 'number': tracking}
            getDhlData_url = 'https://api.dhlglobalmail.com/v2/mailitems/track'
            # Request data from dhl website.
            response = requests.get(getDhlData_url, params=parameters, timeout=5)
            # Another embedded error catch.  Most common 'status_code' returned is 400:  Bad client request.
            if response.status_code != 200:
                print("  >>>  exception caught ... status_code 400 (bad client request) ...")
                return ('exception', 'status_code 400 (bad client request)')

            # If connection is successful break from loop and proceed with script.
            success = True
            break

        # Series of known reoccuring exceptions.
        except requests.exceptions.ConnectionError:
            print("  >>>  dhl connection error (requests.exceptions.ConnectionError) ...")
        except requests.exceptions.ReadTimeout:
            print("  >>>  dhl connection error (requests.exceptions.ReadTimeout) ...")
        except requests.exceptions.RequestsException:
            print("  >>>  dhl connection error (requests.exceptions.RequestsException) ...")
        # A catch all in case there are any more irregular exceptions (no others have yet to be seen).
        except:
            print("  >>>  dhl connection error (other) ...")

        # Some final procedures before looping again.  Along with an update to 'getDhlData_attempts'.
        getDhlData_attempts += 1
        print("  >>>  attempting to reconnect ...\n")
        time.sleep(3)

    # Bad connection catch.  If there are repeated dhl connection errors, then the while-loop breaks without triggering
    # 'success = True', thereby getting caught here and exiting program.
    if not success:  print("  >>>  having repeated issues with dhl connection ... try again later  :("), exit()

    # Convert 'response' to readable dict through json.  Sometimes dhl returns from request an "Expecting ',' delimiter"
    # error.  If exception is caught return "exception" to collect 'tracking-nums' from bad batch.
    try:  response = response.json()
    except json.decoder.JSONDecodeError:
        print("  >>>  exception caught ... json.decoder.JSONDecodeError ...")
        return ('exception', 'json.decoder.JSONDecodeError')

    # Start the data filter, only need contents from 'mailItems'.
    raw_data = response['data']['mailItems']

    # Initiate return variable 'dhl-data' as list of dictionaries of filtered data.  Then start for-loop to handle each
    # entry of 'raw_data' at a time.
    dhl_data = []
    for each in raw_data:
        # Initiate data collection variable to be appended to return variable 'dhl_data'.
        each_dhl_data = {}

        # ...  Note:  Refering to "['events'][0]" when declaring 'time_stamp' and 'message' because the ['events'] list
        # is in chronological order and we always want the data from the most recent event.

        # Get 'time_stamp' from 'date' and 'time' entries in 'raw_data'.
        each_dhl_data['time_stamp'] = each['events'][0]['date'] + ' ' + each['events'][0]['time']

        # Get 'message' from 'raw_data'.
        each_dhl_data['message'] = each['events'][0]['description']
        # Patch to add 'location' to 'message' per Victoria's request.  DL (2019-09-09)
        location = each['events'][0]['location']
        if location != '':
            location = ''.join( l for l in location if l not in ('"', "'") and ord(l) < 128 )
            each_dhl_data['message'] += ' at ' + location

        # Get 'tracking_num' from 'raw_data'.  Have to test 3 different locations for dhl's storage location of
        # 'tracking_number'.
        if str(each['mail']['dspNumber']) in tracking:
            each_dhl_data['tracking_num'] = str(each['mail']['dspNumber'])
        elif str(each['mail']['customerConfirmationNumber']) in tracking:
            each_dhl_data['tracking_num'] = str(each['mail']['customerConfirmationNumber'])
        elif str(each['mail']['overlabeledDspNumber']) in tracking:
            each_dhl_data['tracking_num'] = str(each['mail']['overlabeledDspNumber'])

        # Get 'shipping_id' from 'raw_data'.
        each_dhl_data['shipping_id'] = batch[each_dhl_data['tracking_num']]

        dhl_data.append(each_dhl_data)

    return dhl_data



def updateDatabase(dhl_data, company_id, single=False):
    """
    input:  dhl_data =  List of dicts as batch data pulled from dhl website.  Batch is part of the 'disk_data' whole to
                        be processed.
            single =    Boolean determining style of compiler notes.
    output: Update disk database table 'tblArrival' with data from parameter 'dhl_data'.
    """

    # ... Building sql query.

    # Initial query determining what columns to update on what table and what to do with duplicate entries.
    sql =   """
            INSERT INTO tblArrival (PackageShipmentID, TrackingNumber, MessageTimestamp, Message, CompanyID)
                VALUES {}

            ON DUPLICATE KEY UPDATE
                Message = VALUES(Message),
                LastChecked = NOW(),
                MessageTimestamp = VALUES(MessageTimestamp)
            """
    # Initiate variable used to build values to be inserted into table.
    insert = ""
    # Compiling notes.
    note_list = []
    for index, each in enumerate(dhl_data):
        # Pack data from each 'dhl_data' entry to be formatted into the 'insert' string.
        a, b, c, d, e = each['shipping_id'], each['tracking_num'], each['time_stamp'], each['message'], company_id
        # Compiling notes.
        note_list.append(b.ljust(22))
        # if/else statements used to insert comma at the end of each entry except for the last entry.
        if index != len(dhl_data) - 1:  insert += "('{}', '{}', '{}', '{}', '{}'),".format(a, b, c, d, e)
        else: insert += "('{}', '{}', '{}', '{}', '{}')".format(a, b, c, d, e)

    sql = sql.format(insert)

    ##############################################################                                                      ---  \/  AUTO DEBUG  \/
    if not testing:
        cur.execute(sql)    # <-- final database update statements
        conn.commit()       # <-- final database update statements
        print("  >>>  'tblArrival' updated for ...")
    else:  print("  >>>  <> TESTING <>")
    ##############################################################                                                      ---  /\  AUTO DEBUG  /\

    # Compiling notes.
    if not single:
        note_list += [ "" for each in range(10 - len(note_list)) ]
        print("  >>>  batch size:    ", len(dhl_data))
        print("  >>>  each processed:", note_list[0], note_list[1])
        print("  >>>                 ", note_list[2], note_list[3])
        print("  >>>                 ", note_list[4], note_list[5])
        print("  >>>                 ", note_list[6], note_list[7])
        print("  >>>                 ", note_list[8], note_list[9], "\n")
    else:  print("  >>>  single processed:", note_list[0], "\n")



def batchProcessing(disk_data, company_id):
    """
    input:  disk_data = A dict containing all shipping-numbers and tracking-numbers to be processed.
    output:           - The function's primary job is to perform batch looping on 'disk_data', updating the database and
                        'exceptions' with each batch.
                      - The function also returns 'disk_data', but the dict only contains entries that still need to be
                        processed.
    """
    # Need to make 'exceptions' global to track whole data exceptions.
    global exceptions
    # Variable used to track how many attempts are made to process 'disk_data'.
    while_attempts = 0
    no_movement = False
    previous_to_process = 0

    # Instead of 'while True', this condition to the while-loop is meant also as a catch incase all data is processed
    # with no exceptions (happens sometimes in testing).
    while len(disk_data) > 0:

        while_attempts += 1
        processed = []

        # while-loop controls ...  Tests if a loop has passed with no updates on the data.  If so, break loop to start
        # 'singleProcessing()'.
        if len(disk_data) == previous_to_process:
            print("\n  >>>  *** NO MOVEMENT ON BATCH PROCESSING, STARTING SINGLE PROCESSING ***\n\n")
            break
        # Variable defined to track if while-loop processing has no progress.
        previous_to_process = len(disk_data)

        # Initiate batch variables.
        pivot = size = 10
        batch = {}
        batch_num = 0
        for index, each in enumerate(disk_data):
            # Loading the batch.
            batch[each] = disk_data[each]
            # Batch controls ...  At indexing of 'pivot' or end of 'disk_data', process current batch.
            if index == pivot - 1 or index == len(disk_data) - 1:
                batch_num += 1

                # Compiling notes.
                print("  >>>  still to process (per attempt):", len(disk_data))
                print("  >>>  attempt#:", while_attempts, "... batch#:", batch_num, "...")

                ##################################################################
                ###   \/   FUNCTION CALLS AND DATA PROCESSING PER BATCH   \/   ###
                ##                                                              ##

                dhl_data = getDhlData(batch)

                # Check to see if 'getDhlData()' request threw an exception.  If so, collect batch tracking numbers
                # in 'exceptions' and move to next batch.  If not, continue to process data.
                if dhl_data[0] != "exception":
                    updateDatabase(dhl_data, company_id)
                    # Get list of processed 'shipping_id's from 'dhl_data' to track what entries from 'disk_data' have
                    # been processed.
                    processed += [ d['tracking_num'] for d in dhl_data ]
                else:
                    print("  >>>  processing exception batch ...", dhl_data[1],"...\n")
                    exceptions = { b : dhl_data[1] for b in batch }

                ##                                                              ##
                ###   /\   FUNCTION CALLS AND DATA PROCESSING PER BATCH   /\   ###
                ##################################################################

                # Batch controls ...  Reset 'batch' and adjust 'pivot' by increment of 'size'.
                batch = {}
                pivot += size

        #######################################################################################                         ---  \/  INPUT DEBUG  \/
        #     number_of_batches_to_process = 10 # <-- adjust to limit number of batches processed
        #     if testing and index == number_of_batches_to_process * 10:  break
        # number_of_whileloop_passes = 1 # <-- adjust to limit number of while-loop iterations
        # if testing and while_attempts > number_of_whileloop_passes:  break
        #######################################################################################                         ---  /\  INPUT DEBUG  /\

        # Remove 'processed' from 'disk_data' to not waste time on repeats.  Catch 'KeyError's (still not sure why
        # they're occurring).
        for p in processed:
            try:  del disk_data[p]
            except KeyError:
                print("  >>>  exception caught ... KeyError ...\n")
                exceptions[each] = "KeyError"

    # Return what is left in 'disk_data' still to be processed for 'singleProcessing()'.
    return disk_data
