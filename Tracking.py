
"""

Tracking.py (module)

- 2020-02-08 by David Lang
    - updated method:
        - getSingleDhlJson() = Minor adjustments.
    - created methods:
        - getSingleDhlVitals(), getSingleDhlHistory() = Formatted return to conform to standard
                                                        format.

- 2020-01-06 by David Lang
    - updated methods:
        - getSingleUpsJson(), getSingleUpsVitals(), getSingleUpsHistory() = Updated error handling.
        - getSingleUspsHistory() =  Was missing most recent event from 'TrackSummary'.  And now more
                                    efficient with use of subroutine.

- 2019-12-26 by David Lang
    - updated methods:
        - getSingleUspsJson() = Now using USPSApi module for easier json, message, and time stamp
                                parsing.
        - getSingleUspsVitals() = Had to update parsing due to update to getSingleUspsJson().
    - created method:
        - getSingleUspsHistory() = From getSingleUspsJson() parse out and return tracking history.

- 2019-12-13 by David Lang
    - updated methods:
        - getSingleUpsJson() =  Introduce argument 'activity_type' as conditional for retrieving
                                most recent activity or all activity; a necessary update for
                                getSingleUpsHistory().
    - created method:
        - getSingleUpsHistory() = From getSingleUpsJson() parse out and return tracking history.

- 2019-11-20 by David Lang
    - created methods:
        - getSingleUpsJson()        = Call UPS API and return json of argued tracking number.
        - getSingleUpsVitals()      = From getSingleUpsJson() parse out and return vital data.
        - getSingleUspsJson()       = Call UPS API and retrieve json of argued tracking number.
        - getSingleUspsVitals()     = From getSingleUspsJson() parse out and return vital data.
        - getSingleFedExJson()      = Call FedEx API and retrieve json of argued tracking number.
        - getSingleFedExVitals()    = From getSingleFedExJson() parse out and return vital data.

"""



####################################################################################################
                                                                                 ###   IMPORTS   ###
                                                                                 ###################



""" KEEP FOR EXPORTING """
# import sys
# sys.path.insert(0, '/tomcat/python')

# Patch for issues with 'urllib.request.Request'.
import ssl
try:  _create_unverified_https_context = ssl._create_unverified_context
except AttributeError:  pass
else:  ssl._create_default_https_context = _create_unverified_https_context

import json
import xmltodict
import requests
import time
import TrackingCredentials as cred
from datetime import datetime
from urllib.request import Request, urlopen
from usps import USPSApi



####################################################################################################
                                                                               ###   CONSTANTS   ###
                                                                               #####################



# getSingleUpsJson()
UPS_ACCESS_LICENSE_NUMBER = cred.UPS_ACCESS_LICENSE_NUMBER
UPS_USER_ID = cred.UPS_USER_ID
UPS_PASSWORD = cred.UPS_PASSWORD
UPS_ONLINETOOLS_URL = 'https://onlinetools.ups.com/ups.app/xml/Track'
UPS_REQUEST_HEADERS = {'Content-Type': 'application/x-www-form-urlencoded'}
UPS_MAIL_INNOVATION_TAG = '<IncludeMailInnovationIndicator/>'

# getSingleUspsJson()
USPS_USER_ID = cred.USPS_USER_ID
USPS_REQUEST_ATTEMPTS = 5
USPS_REQUEST_DELAY = 0.2

USPS_DELIVERED_MESSAGES = ['Delivered', 'Available for Pickup']

""" obsolete 2019-12-26 """
# # getSingleUspsVitals()
# USPS_DELIVERED_MESSAGES = [
#     'Your item was delivered',                  'Your item has been delivered',
#     'We attempted to deliver your item',        'Your item was picked up',
#     'Your item is being held',                  'Your item was forwarded',
#     'The return on your item was processed',    'Your item was returned'
# ]

# getSingleDhlJson()
DHL_USERNAME = cred.DHL_USERNAME
DHL_PASSWORD = cred.DHL_PASSWORD
DHL_CLIENT_ID = cred.DHL_CLIENT_ID

# getSingleDhlVitals()
DHL_DELIVERED_MESSAGES = ['DELIVERED']

# getSingleFedexJson()
FEDEX_KEY = cred.FEDEX_KEY
FEDEX_PASSWORD = cred.FEDEX_PASSWORD
FEDEX_ACCOUNT_NUMBER = cred.FEDEX_ACCOUNT_NUMBER
FEDEX_METER_NUMBER = cred.FEDEX_METER_NUMBER

# getSingleFedExVitals()
FEDEX_DELIVERED_MESSAGES = ['Delivered']



####################################################################################################
                                                                                 ###   METHODS   ###
                                                                                 ###################



def removeNonAscii(string, replace=''):
    """
    input:  string = String to have non-ascii characters removed.
            replace = String to replace into 'string' where non-ascii characters are found.
    output: String of 'string', where non-ascii characters have been replaced by 'replace'.
    """
    if not all(ord(char) < 128 for char in string):
        return ''.join( char if ord(char) < 128 else replace for char in string )
    else:
        return string



####################################################################################################
                                                                           ###   METHODS / UPS   ###
                                                                           #########################



def getSingleUpsJson(_tracking_number, activity_type='last'):
    """
    input:  constants =         UPS_ACCESS_LICENSE_NUMBER, UPS_USER_ID, UPS_PASSWORD,
                                UPS_ONLINETOOLS_URL, UPS_REQUEST_HEADERS, UPS_MAIL_INNOVATION_TAG
            _tracking_number =  UPS tracking number to be sent to UPS API to recover tracking data.
            activity_type =     Currently accepts 'last' or 'all'.  'last' returns json of most
                                recent tracking activity.  'all' returns json of all tracking
                                activity.
    output: Return json 'ups_data_', of response from UPS API for input '_tracking_number' and
            'activity_type'.
    """

    # Handling of string arg 'activity_type'.
    if activity_type == 'last':
        customer_context, request_option = 'get tracking last activity', '0'
    elif activity_type == 'all':
        customer_context, request_option = 'get tracking all activity', '1'
    else:
        err_msg =  "ERROR:  Acceptable values for argument 'activity_type' are 'last' (default) or"
        err_msg += " 'all', not '{}'.".format(activity_type)
        return err_msg

    xml = """
        <AccessRequest xml:lang="en-US">
            <AccessLicenseNumber>{}</AccessLicenseNumber>
            <UserId>{}</UserId>
            <Password>{}</Password>
        </AccessRequest>
        <?xml version="1.0"?>
        <TrackRequest xml:lang="en-US">
            <Request>
                <TransactionReference>
                    <CustomerContext>{}</CustomerContext>
                </TransactionReference>
                <XpciVersion>1.0</XpciVersion>
                <RequestAction>Track</RequestAction>
                <RequestOption>{}</RequestOption>
            </Request>
            {}
            <TrackingNumber>{}</TrackingNumber>
        </TrackRequest>
    """
    # 'MAIL_INNOVATION_TAG' is an xml tag needed to designate that 'TrackingNumber' is for an
    # envelope package type.
    xml = xml.format(
        UPS_ACCESS_LICENSE_NUMBER, UPS_USER_ID, UPS_PASSWORD, customer_context, request_option,
        UPS_MAIL_INNOVATION_TAG, _tracking_number
    ).encode('utf-8')

    ups_data_ = Request(url=UPS_ONLINETOOLS_URL, data=xml, headers=UPS_REQUEST_HEADERS)
    ups_data_ = urlopen(ups_data_).read()
    # Convert 'xml' to 'json'.
    ups_data_ = json.loads(json.dumps(xmltodict.parse(ups_data_)))

    return ups_data_



def getSingleUpsVitals(_tracking_number):
    """
    input:  _tracking_number = UPS tracking number to be sent to UPS API to recover tracking data.
    output: vitals_['delivered'] =  Bool, True if package has been delivered, else False.
            vitals_['message'] =    String, of most recently updated tracking message.
            vitals_['time_stamp'] = Datetime object, of time stamp when 'details_['message']' was
                                    created.
    """

    delivered, message, time_stamp = False, '', ''

    # Get the json pack from UPS API and start parsing.
    ups_data = getSingleUpsJson(_tracking_number)
    # 'if' block handles bad tracking numbers.
    if 'Error' in ups_data['TrackResponse']['Response']:
        message = ups_data['TrackResponse']['Response']['Error']['ErrorDescription']
        vitals_ = {'delivered': delivered, 'message': message, 'time_stamp': time_stamp}
        return vitals_
    ups_data = ups_data['TrackResponse']['Shipment']
    # Check for odd json response format; sometimes ['Shipment'] is a list-of-dicts, not dict.
    if isinstance(ups_data, dict):  ups_data = ups_data['Package']
    else:  ups_data = ups_data[0]['Package']

    # Toggle 'delivered' from value of ['DeliveryIndicator'].
    if ups_data['DeliveryIndicator'] == 'Y':  delivered = True

    # Get 'message_' directly from ['Description'].
    message = ups_data['Activity']['Status']['StatusType']['Description']
    # try/except uses 'removeNonAscii()' to clean string 'message'.
    try:
        print(">>> looking for non-ascii...", message)
    except UnicodeEncodeError:
        message = removeNonAscii(message)
        print(">>> non-ascii found...", message)
    # Build 'location' from components of ['Address'].
    location = ups_data['Activity']['ActivityLocation']['Address']
    loc_keys = ['City', 'StateProvinceCode', 'CountryCode', 'PostalCode']
    location = ' '.join([ location[key] for key in loc_keys if key in location ])
    message += ' - ' + location

    # Build 'date' as datetime object from ['Date'].
    time_stamp = ups_data['Activity']['Date']
    time_stamp = datetime.strptime(time_stamp, '%Y%m%d')

    vitals_ = {'delivered': delivered, 'message': message, 'time_stamp': time_stamp}
    return vitals_



def getSingleUpsHistory(_tracking_number):
    """
    input:  _tracking_number = UPS tracking number to be sent to UPS API to recover tracking data.
    output: Return list-of-dicts 'history_'.  Each entity of list is a tracking event in the
            packages history.  Each entity's dictionary is comprised of:
                'message' = String of message of event.
                'location' = String of location of event.
                'time_stamp' = Datetime object of time stamp of event.
    """

    # Get the json pack from UPS API and start parsing.
    ups_data = getSingleUpsJson(_tracking_number, 'all')
    # 'if' block handles bad tracking numbers.
    if 'Error' in ups_data['TrackResponse']['Response']:
        return [{
            'message': ups_data['TrackResponse']['Response']['Error']['ErrorDescription'],
            'location': '',
            'time_stamp': ''
        }]
    ups_data = ups_data['TrackResponse']['Shipment']['Package']['Activity']

    # Populate return object 'history_' with data from iterations of 'ups_data'.
    history_ = []
    for activity in ups_data:
        message, location, time_stamp = '', '', ''

        # Get 'history_['message']' value.
        message = activity['Status']['StatusType']['Description']

        # Get 'history_['location']' value.
        activity_loc = activity['ActivityLocation']['Address']
        loc_keys = ['City', 'StateProvinceCode', 'CountryCode', 'PostalCode']
        location = ' '.join([ activity_loc[key] for key in loc_keys if key in activity_loc ])

        # Get 'history_['time_stamp']' value.
        time_stamp = datetime.strptime(activity['Date'] + activity['Time'], '%Y%m%d%H%M')

        # Iterate build of 'history_'.
        history_ += [{ 'message': message, 'location': location, 'time_stamp': time_stamp }]

    return history_



####################################################################################################
                                                                          ###   METHODS / USPS   ###
                                                                          ##########################



def getSingleUspsJson(_tracking_number):
    """
    input:  constants = USPS_REQUEST_ATTEMPTS, USPS_USER_ID, USPS_REQUEST_DELAY
            _tracking_number = USPS tracking number to be sent to USPS API to recover tracking data.
    output: Return json 'usps_data_', of response from USPS API for input '_tracking_number'.
    """

    usps_data_ = {}
    for i in range(USPS_REQUEST_ATTEMPTS):
        try:
            usps = USPSApi(USPS_USER_ID)
            usps_data_ = usps.track(_tracking_number).result
            break
        except requests.exceptions.RequestException:
            print("\n>>> RequestException ... Trying again ...\n")
            time.sleep(5)
            continue

    # If too many request exceptions, give error message in working return json format.
    if not usps_data_:  exit("\n>>> ERROR:  Too many request exceptions ... \\_(**)_/\n")

    time.sleep(USPS_REQUEST_DELAY)

    return usps_data_



def getSingleUspsVitals(_tracking_number):
    """
    input:  constants = USPS_DELIVERED_MESSAGES
            _tracking_number = USPS tracking number to be sent to USPS API to recover tracking data.
    output: vitals_['delivered'] =  Bool, True if package has been delivered, else False.
            vitals_['message'] =    String, of most recently updated tracking message.
            vitals_['time_stamp'] = Datetime object, of time stamp when 'vitals_['message']' was
                                    created.
    """

    vitals_ = { 'delivered': False, 'message': '', 'time_stamp': '' }

    # Get the json pack from USPS API and start parsing.
    usps_data = getSingleUspsJson(_tracking_number)
    usps_data = usps_data['TrackResponse']['TrackInfo']
    # 'if' block handles bad tracking numbers.
    if 'Error' in usps_data:
        vitals_['message'] = usps_data['Error']['Description']
        return vitals_
    usps_data = usps_data['TrackSummary']

    # Get 'message'.
    vitals_['message'] = usps_data['Event']
    for loc in ['EventCity', 'EventState', 'EventCountry']:
        if usps_data[loc] != None:  vitals_['message'] += ' ' + usps_data[loc]

    # Get 'delivered'.
    for message in USPS_DELIVERED_MESSAGES:
        if message in vitals_['message']:
            vitals_['delivered'] = True
            break

    # Get 'time_stamp', if/else handles possible missing 'EventTime'.
    if usps_data['EventTime'] != None:
        date_time = usps_data['EventDate'] + usps_data['EventTime']
        vitals_['time_stamp'] = datetime.strptime(date_time, '%B %d, %Y%I:%M %p')
    else:
        vitals_['time_stamp'] = datetime.strptime(usps_data['EventDate'], '%B %d, %Y')

    return vitals_



def getSingleUspsHistory(_tracking_number):
    """
    input:  _tracking_number = USPS tracking number to be sent to USPS API to recover tracking data.
    output: Return list-of-dicts 'history_'.  Each entity of list is a tracking event in the
            package's history.  Each entity's dictionary is comprised of:
                'message' = String of message of event.
                'location' = String of location of event.
                'time_stamp' = Datetime object of time stamp of event.
    """

    def parseEventDict(_event):
        """ subroutine: Parse and return selected data from json event dict. """
        message, location, time_stamp = '', '', ''

        # Get 'message'.
        message = _event['Event']
        # Get 'location'.
        loc_keys = ['EventCity', 'EventState', 'EventCountry', 'EventZIPCode']
        location = ' '.join([ _event[key] for key in loc_keys if _event[key] != None ])
        # Get 'time_stamp'.
        if _event['EventTime'] != None:
            date_time = _event['EventDate'] + _event['EventTime']
            time_stamp = datetime.strptime(date_time, '%B %d, %Y%I:%M %p')
        else:
            time_stamp = datetime.strptime(_event['EventDate'], '%B %d, %Y')

        return {'message': message, 'location': location, 'time_stamp': time_stamp}

    # Get json from USPS API and start parsing.
    usps_data = getSingleUspsJson(_tracking_number)
    usps_data = usps_data['TrackResponse']['TrackInfo']
    # 'if' block handles bad tracking numbers.
    if 'Error' in usps_data:
        return [{'message': usps_data['Error']['Description'], 'location': '', 'time_stamp': ''}]

    history_ = []

    # Build 'history_' with most recent event from 'TrackSummary'.
    history_ += [parseEventDict(usps_data['TrackSummary'])]
    # Build 'history_' by iterating over events from 'TrackDetail'.
    for details in usps_data['TrackDetail']:  history_ += [parseEventDict(details)]

    return history_



####################################################################################################
                                                                           ###   METHODS / DHL   ###
                                                                           #########################



def getSingleDhlJson(_tracking_number):
    """
    input:  _tracking_number = DHL tracking number to be sent to DHL API to recover tracking data.
    output: Return json 'dhl_data_', of response from DHL API for input '_tracking_number'.
    """

    def getDhlKey():
        """ subroutine:  """
        # Build variables for get request.
        headers = {'Content-Type': 'application/json'}
        parameters = {'username': DHL_USERNAME, 'password': DHL_PASSWORD}
        url = 'https://api.dhlglobalmail.com/v1/auth/access_token/'
        # Perform request and filter to return 'key_'.
        key_ = requests.get(url, headers=headers, params=parameters, timeout=5).json()
        key_ = key_['data']['access_token']

        return key_

    # Build output parameters for retrieving dhl data.
    params = {'access_token': getDhlKey(), 'client_id': DHL_CLIENT_ID, 'number': _tracking_number}
    url = 'https://api.dhlglobalmail.com/v2/mailitems/track'
    # Request data from dhl website, then convert to json.
    dhl_data_ = requests.get(url, params=params, timeout=5).json()

    return dhl_data_



def getSingleDhlVitals(_tracking_number):
    """
    input:  constants = DHL_DELIVERED_MESSAGES
            _tracking_number = DHL tracking number to be sent to DHL API to recover tracking data.
    output: vitals_['delivered'] =  Bool, True if package has been delivered, else False.
            vitals_['message'] =    String, of most recently updated tracking message.
            vitals_['time_stamp'] = Datetime object, of time stamp when 'details_['message']' was
                                    created.
    """
    delivered, message, time_stamp = False, '', ''

    # Get json, check for errors, then start parsing.
    dhl_data = getSingleDhlJson(_tracking_number)
    # if conditioning used for error handling of bad tracking numbers.
    if 'error' in dhl_data['meta']:
        message = dhl_data['meta']['error'][0]['error_message']
        vitals_ = {'delivered': delivered, 'message': message, 'time_stamp': time_stamp}
        return vitals_
    dhl_data = dhl_data['data']['mailItems'][0]['events'][0]

    # Get 'delivered'.
    for del_mes in DHL_DELIVERED_MESSAGES:
        if del_mes in dhl_data['description']:
            delivered = True
            break

    # Get 'message'.
    message = dhl_data['description']
    location = dhl_data['location'].replace(',', '') + ' ' + str(dhl_data['postalCode'])
    message += ' - ' + location

    # Get 'time_stamp'.
    time_stamp = dhl_data['date'] + dhl_data['time']
    time_stamp = datetime.strptime(time_stamp[:-3], '%Y-%m-%d%H:%M')

    vitals_ = {'delivered': delivered, 'message': message, 'time_stamp': time_stamp}
    return vitals_



def getSingleDhlHistory(_tracking_number):
    """
    input:  _tracking_number = DHL tracking number to be sent to DHL API to recover tracking data.
    output: Return list-of-dicts 'history_'.  Each entity of list is a tracking event in the
            packages history.  Each entity's dictionary is comprised of:
                'message' = String of message of event.
                'location' = String of location of event.
                'time_stamp' = Datetime object of time stamp of event.
    """

    # Get the json pack from DHL API and start parsing.
    dhl_data = getSingleDhlJson(_tracking_number)
    # if condition used for error handling of bad tracking numbers.
    if 'error' in dhl_data['meta']:
        message = dhl_data['meta']['error'][0]['error_message']
        history_ = [{'message': message, 'location': '', 'time_stamp': ''}]
        return history_
    dhl_data = dhl_data['data']['mailItems'][0]['events']

    history_ = []
    for event in dhl_data:
        message, location, time_stamp = '', '', ''

        # Get 'message'.
        message = event['description']
        # Get 'location'.
        location = event['location'].replace(',', '') + ' ' + str(event['postalCode'])
        if location == ' ':  location = ''
        # Get 'time_stamp'.
        time_stamp = event['date'] + event['time']
        time_stamp = datetime.strptime(time_stamp[:-3], '%Y-%m-%d%H:%M')

        # Iterate build of 'history_'.
        history_ += [{ 'message': message, 'location': location, 'time_stamp': time_stamp }]

    return history_



####################################################################################################
                                                                         ###   METHODS / FEDEX   ###
                                                                         ###########################



#########################################################
""" <> CAUTION <> UNDER CONSTRUCTION <> KEEP CLEAR <> """
### \/ ####### \/ ################## \/ ########## \/ ###



def getSingleFedExJson(_tracking_number):
    """
    input:  constants = FEDEX_ACCOUNT_NUMBER, FEDEX_METER_NUMBER, FEDEX_KEY, FEDEX_PASSWORD
            _tracking_number =  FedEx tracking number to be sent to FedEx API to recover tracking
                                data.
    output: Return json 'fedex_data_' of response from FedEx API for input '_tracking_number'.
    """

    xml = """
        <soapenv:Envelope
            xmlns:soapenv="http://schemas.xmlsoap.org/soap/envelope/"
            xmlns:v14="http://fedex.com/ws/track/v14"
        >
            <soapenv:Header/>
            <soapenv:Body>
                <v14:TrackRequest>
                    <v14:WebAuthenticationDetail>
                        <v14:ParentCredential>
                            <v14:Key>{fk}</v14:Key>
                            <v14:Password>{fp}</v14:Password>
                        </v14:ParentCredential>
                        <v14:UserCredential>
                            <v14:Key>{fk}</v14:Key>
                            <v14:Password>{fp}</v14:Password>
                        </v14:UserCredential>
                    </v14:WebAuthenticationDetail>
                    <v14:ClientDetail>
                        <v14:AccountNumber>{}</v14:AccountNumber>
                        <v14:MeterNumber>{}</v14:MeterNumber>
                    </v14:ClientDetail>
                    <v14:TransactionDetail>
                        <v14:CustomerTransactionId>Track By Number_v14</v14:CustomerTransactionId>
                        <v14:Localization>
                            <v14:LanguageCode>EN</v14:LanguageCode>
                            <v14:LocaleCode>US</v14:LocaleCode>
                        </v14:Localization>
                    </v14:TransactionDetail>
                    <v14:Version>
                        <v14:ServiceId>trck</v14:ServiceId>
                        <v14:Major>14</v14:Major>
                        <v14:Intermediate>0</v14:Intermediate>
                        <v14:Minor>0</v14:Minor>
                    </v14:Version>
                    <v14:SelectionDetails>
                        <v14:CarrierCode>FDXE</v14:CarrierCode>
                        <v14:PackageIdentifier>
                            <v14:Type>TRACKING_NUMBER_OR_DOORTAG</v14:Type>
                            <v14:Value>{}</v14:Value>
                        </v14:PackageIdentifier>
                        <v14:ShipmentAccountNumber/>
                        <v14:SecureSpodAccount/>
                        <v14:Destination>
                            <v14:GeographicCoordinates>
                                rates evertitque aequora
                            </v14:GeographicCoordinates>
                        </v14:Destination>
                    </v14:SelectionDetails>
                </v14:TrackRequest>
            </soapenv:Body>
        </soapenv:Envelope>
    """
    xml = xml.format(
        FEDEX_ACCOUNT_NUMBER, FEDEX_METER_NUMBER, _tracking_number, fk=FEDEX_KEY, fp=FEDEX_PASSWORD
    )
    request_url = 'https://ws.fedex.com:443/web-services'
    params = {'content-type': 'application/soap+xml'}
    httpresq = Request(url=request_url, data=xml.encode('utf-8'), headers=params)
    response = urlopen(httpresq).read().decode('utf-8')
    fedex_data_ = json.loads(json.dumps(xmltodict.parse(response)))

    return fedex_data_



def getSingleFedExVitals(_tracking_number):
    """
    input:  _tracking_number =  FedEx tracking number to be sent to FedEx API to recover tracking
                                data.
    output: details_['delivered'] =     Bool, True if package has been delivered, else False.
            details_['message'] =       String, of most recently updated tracking message.
            details_['time_stamp'] =    Datetime object, of time stamp when 'details_['message']'
                                        was created.
    """

    delivered, message, time_stamp = False, '', datetime(1, 1, 1)

    # Get the json pack from FedEx API and start parsing.
    fedex_data = getSingleFedExJson(_tracking_number)
    fedex_data = fedex_data['SOAP-ENV:Envelope']['SOAP-ENV:Body']['TrackReply']
    fedex_data = fedex_data['CompletedTrackDetails']['TrackDetails']['Events']

    message = fedex_data['EventDescription']
    # Extract bool 'delivered'.
    for dm in FEDEX_DELIVERED_MESSAGES:
        if message.startswith(dm):
            delivered = True
            break
    # Extract string 'location'.
    location = fedex_data['Address']
    loc_keys = ['City', 'StateOrProvinceCode', 'CountryCode', 'PostalCode']
    location = ' '.join([ location[key] for key in loc_keys if key in location ])
    # Concatenate 'message' with 'location'.
    message = message + ' at ' + location
    # try/except uses 'removeNonAscii()' to clean string 'message'.
    try:
        print(">>> looking for non-ascii...", message)
    except UnicodeEncodeError:
        message = removeNonAscii(message)
        print(">>> non-ascii found...", message)
    # Extract 'time_stamp', then convert to datetime object.
    time_stamp = fedex_data['Timestamp']
    time_stamp = time_stamp[:time_stamp.rfind('-')]
    time_stamp = datetime.strptime(time_stamp, '%Y-%m-%dT%H:%M:%S')

    details_ = {'delivered': delivered, 'message': message, 'time_stamp': time_stamp}
    return details_



### /\ ####### /\ ################## /\ ########## /\ ###
""" <> CAUTION <> UNDER CONSTRUCTION <> KEEP CLEAR <> """
#########################################################



""" obsolete 2019-12-26 """
# def getSingleUspsJson(_tracking_number):
#     """
#     input:  constants = USPS_USER_ID, USPS_REQUEST_DELAY
#             _tracking_number = SUPS tracking number to be sent to USPS API to recover tracking data.
#     output: Return json 'usps_data_', of response from USPS API for input '_tracking_number'.
#     """
#
#     url = 'http://production.shippingapis.com/ShippingAPI.dll'
#     xml = """
#         <? xml version="1.0" encoding="UTF-8" ?>
#         <TrackRequest USERID="{}">
#             <TrackID ID="{}"></TrackID>
#         </TrackRequest>
#     """.format(USPS_USER_ID, _tracking_number)
#     parameters = {'API': 'TrackV2', 'XML': xml}
#
#     # Attempt to connect with USPS API.  With a 3 second timeout window, if 5 attempts are made
#     # resulting in timeouts or connection errors then the program exits.
#     attempts = 0
#     time.sleep(USPS_REQUEST_DELAY)
#     try:
#         response = requests.get(url, params=parameters, timeout=3).text
#     # except (requests.exceptions.ConnectTimeout, ConnectionError, requests.exceptions.ReadTimeout):
#     except:
#         attempts += 1
#         if attempts == 5:
#             exit(">>> too many timeouts, something's wrong, exiting program...")
#         print("\n>>> connection error, trying again...\n")
#         time.sleep(3)
#         response = requests.get(url, params=parameters, timeout=3).text
#     # Convert 'xml' to 'json'.
#     usps_data_ = json.loads(json.dumps(xmltodict.parse(response)))
#
#     return usps_data_



""" obsolete 2019-12-26 """
# def extractUspsTimeStamp(_message):
#     """
#     input:  _message = String from USPS API describing most recent shipment activity.
#     output: Return datetime object derived from a variety of slice formats from the input
#             '_message'.  The USPS API outputs many various formats for their shipment activity
#             messages.  This function sorts through '_message' to find the correct values for the
#             datetime object.
#     """
#     # The primary time stamp indicator from '_message' is the ':' from hours/minutes.  So, we first
#     # find the index of ':' as a starting point.  If no ':', return empty string.
#     colon = _message.find(':')
#     if colon == -1:  return ''
#     # Get number of digit places for hours.
#     if _message[colon - 2] == '1':  hour_digits = 2
#     else:                           hour_digits = 1
#
#     # if/else sorts between '_message' formats of time-before-date and date-before-time.
#     if _message[colon:].find('on') == 7:
#         # If time-before-date, then we quickly get 'time_string' with 'begin' value starting
#         # immediately before hours, and 'end' value is found 6 digits beyond ','.
#         begin = colon - hour_digits
#         end = colon + _message[colon:].find(',') + 6
#         time_string = _message[begin:end]
#
#         return datetime.strptime(time_string, '%I:%M %p on %B %d, %Y')
#
#     else:
#         # There are 2 possible variations to the date-before-time format.  One is preceded with 'of'
#         # and parsed with ',', the other is preceded with 'on' and parsed with 'at'.  These if/else
#         # conditions determine which this '_message' is, using 'cut_point' as reference.  Then
#         # assigns values to 'cut_more' and 'look_for' for use in slicing out 'time_string'
#         # from '_message'.
#         cut_point = colon - hour_digits - 2
#         if _message[cut_point] == ',':  cut_more, look_for = 0, 'of'
#         else:                           cut_more, look_for = 2, 'on'
#         time_string = _message[:cut_point - cut_more] + _message[cut_point + 1:]
#         # Have to reset value of 'colon' due to previous slicing.
#         colon = time_string.find(':')
#         begin = time_string[:colon].rfind(look_for) + 3
#         end = colon + 6
#         time_string = time_string[begin:end]
#
#         return datetime.strptime(time_string, '%B %d, %Y %I:%M %p')



""" obsolete 2019-12-26 """
# def getSingleUspsVitals(_tracking_number):
#     """
#     input:  constants = DELIVERED_MESSAGES
#             _tracking_number = USPS tracking number to be sent to USPS API to recover tracking data.
#     output: details_['delivered'] =     Bool, True if package has been delivered, else False.
#             details_['message'] =       String, of most recently updated tracking message.
#             details_['time_stamp'] =    Datetime object, of time stamp when 'details_['message']'
#                                         was created.
#     """
#
#     # Get the json pack from USPS API and parse 'message' with try/except for common errors.
#     usps_data = getSingleUspsJson(_tracking_number)
#     try:
#         message = usps_data['TrackResponse']['TrackInfo']['TrackSummary']
#     except KeyError:
#         print(">>> bad response, ignoring shipment...\n")
#         return "bad response"
#
#     # try/except uses 'removeNonAscii()' to clean string 'message'.
#     try:
#         print(">>> looking for non-ascii...", message)
#     except UnicodeEncodeError:
#         message = removeNonAscii(message)
#         print(">>> non-ascii found...", message)
#     # Extract bool 'delivered' from 'message' using 'DELIVERED_MESSAGES'.
#     delivered = False
#     for dm in USPS_DELIVERED_MESSAGES:
#         if message.startswith(dm):
#             delivered = True
#             break
#     # Extract 'time_stamp' from 'message'.
#     time_stamp = extractUspsTimeStamp(message)
#     vitals_ = {'delivered': delivered, 'message': message, 'time_stamp': time_stamp}
#
#     return vitals_
