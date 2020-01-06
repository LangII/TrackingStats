
import Tracking
import json



# ups_track_num_bad = '1Z6TT600YW11610782' # UPS SURE POST Over 1LB
# ups_track_num_bad = '1Z6567640338912735' # UPS Ground

ups_track_num_good = '92612901204900553018959772' # UPS MI Parcel Select
# ups_track_num_good = '92748901204900553018960507' # UPS MI Dom
# ups_track_num_good = '92419901204900553019093456' # UPS MI BPM



usps_track_num_bad = ''
usps_track_num_good = '9449009205987503039673'



# ups_json = Tracking.getSingleUpsJson(ups_track_num_good)
# print(json.dumps(ups_json, indent=4, sort_keys=True))

# ups_vitals = Tracking.getSingleUpsVitals(ups_track_num_good)
# print(ups_vitals)

# ups_history = Tracking.getSingleUpsHistory(ups_track_num_good)
# for event in ups_history:  print(event)



# usps_json = Tracking.getSingleUspsJson(usps_track_num_good)
# print(json.dumps(usps_json, indent=4, sort_keys=True))

# usps_vitals = Tracking.getSingleUpsVitals(usps_track_num_good)
# print(usps_vitals)

usps_history = Tracking.getSingleUspsHistory(usps_track_num_good)
for event in usps_history:  print(event)
