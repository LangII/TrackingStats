
import Tracking
import json







# ups_track_num_bad = '1Z6TT600YW11610782' # UPS SURE POST Over 1LB
# ups_track_num_bad = '1Z6567640338912735' # UPS Ground
ups_track_num_bad = '92748901204900553008609362' # UPS MI Dom

# ups_track_num_good = '92612901204900553019204505' # UPS MI Parcel Select
# ups_track_num_good = '92748901204900553018974214' # UPS MI Dom
ups_track_num_good = '92419901204900553019449321' # UPS MI BPM



# ups_json = Tracking.getSingleUpsJson(ups_track_num_good)
# print(json.dumps(ups_json, indent=4, sort_keys=True))
# print("\n\n\n")
#
# ups_vitals = Tracking.getSingleUpsVitals(ups_track_num_good)
# print(ups_vitals)
# print("\n\n\n")
#
# ups_history = Tracking.getSingleUpsHistory(ups_track_num_good)
# for event in ups_history:  print(event)
# print("\n\n\n")



# usps_track_num_bad = ''

usps_track_num_good = '9405510298023827496'



# usps_json = Tracking.getSingleUspsJson(usps_track_num_good)
# print(json.dumps(usps_json, indent=4, sort_keys=True))
# print("\n\n\n")
#
# usps_vitals = Tracking.getSingleUspsVitals(usps_track_num_good)
# print(usps_vitals)
# print("\n\n\n")
#
# usps_history = Tracking.getSingleUspsHistory(usps_track_num_good)
# for event in usps_history:  print(event)
# print("\n\n\n")



dhl_track_num_good = '92748902233537028873'



dhl_json = Tracking.getSingleDhlJson(dhl_track_num_good)
print(json.dumps(dhl_json, indent=4, sort_keys=True))
print("\n\n\n")

dhl_vitals = Tracking.getSingleDhlVitals(dhl_track_num_good)
print(dhl_vitals)
print("\n\n\n")



# fedex_track_num_good = '058793380242487'

# fedex_json = Tracking.getSingleFedExJson(fedex_track_num_good)
# print(json.dumps(fedex_json, indent=4, sort_keys=True))
# print("\n\n\n")



####################################################################################################



# from Required import Connections
# conn = Connections.connect()
# cur = conn.cursor()
#
# for i in range(1, 13):
#     date_stamp = '2018-{}-01'
#     greater = date_stamp.format(str(i).rjust(2, '0'))
#     if i != 12: less = date_stamp.format(str(i + 1).rjust(2, '0'))
#     else:       less = '2019-01-01'
#     query = """
#         SELECT COUNT(1) FROM tblArrival
#             WHERE MessageTimestamp > '{gt}'
#                 AND MessageTimestamp < '{lt}'
#     """
#     query = query.format(gt=greater, lt=less)
#     cur.execute(query)
#     select = cur.fetchone()
#     print("counts of", greater, "to", less, "are", select)
# exit()
