
"""

TrackingSettings.py

"""



####################################################################################################

####################################################################################################



"""

NOTES:
- Set ['start_date'] and ['end_date'] to '' for values to be auto generated by 'days_ago'.

"""

update_tracking_series = [
    {
        'company_id': 507, 'shipped_method': 'USPS Media Mail', 'days_ago': 30, 'start_date': '',
        'end_date': ''
    },
    {
        'company_id': 735, 'shipped_method': 'USPS Media Mail', 'days_ago': 30, 'start_date': '',
        'end_date': ''
    },
    {
        'company_id': 1232, 'shipped_method': 'USPS Media Mail', 'days_ago': 30, 'start_date': '',
        'end_date': ''
    },
    {
        'company_id': 1899, 'shipped_method': 'UPS MI BPM', 'days_ago': 30, 'start_date': '',
        'end_date': ''
    },
    {
        'company_id': 1584, 'shipped_method': 'UPS MI Dom', 'days_ago': 30, 'start_date': '',
        'end_date': ''
    },
    {
        'company_id': 1236, 'shipped_method': 'UPS MI Dom', 'days_ago': 30, 'start_date': '',
        'end_date': ''
    },
    {
        'company_id': 752, 'shipped_method': 'UPS MI Dom', 'days_ago': 30, 'start_date': '',
        'end_date': ''
    },
    {
        'company_id': 816, 'shipped_method': 'USPS Priority', 'days_ago': 30, 'start_date': '',
        'end_date': ''
    },
    {
        'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'days_ago': 30,
        'start_date': '', 'end_date': ''
    },
]



####################################################################################################

####################################################################################################



"""

NOTES:
 - If ['start_date'] and ['end_date'] are empty, script will auto fill with most recent Sundays.
 - ['date_range_type'] has to be 'day', 'week', 'month', or 'custom'.
 - (for now) ONLY set ['max_freq'] to 14!

 """

generate_freq_dist_series = [
    {
        'company_id': 507, 'shipped_method': 'USPS Media Mail', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '', 'end_date': ''
    },
    {
        'company_id': 735, 'shipped_method': 'USPS Media Mail', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '', 'end_date': ''
    },
    {
        'company_id': 1232, 'shipped_method': 'USPS Media Mail', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '', 'end_date': ''
    },
    {
        'company_id': 1899, 'shipped_method': 'UPS MI BPM', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '', 'end_date': ''
    },
    {
        'company_id': 1584, 'shipped_method': 'UPS MI Dom', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '', 'end_date': ''
    },
    {
        'company_id': 1236, 'shipped_method': 'UPS MI Dom', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '', 'end_date': ''
    },
    {
        'company_id': 752,  'shipped_method': 'UPS MI Dom', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '', 'end_date': ''
    },
    {
        'company_id': 816, 'shipped_method': 'USPS Priority', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '', 'end_date': ''
    },
    {
        'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '', 'end_date': ''
    },
]

# generate_freq_dist_series = [
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-12-15', 'end_date': '2019-12-22'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-12-08', 'end_date': '2019-12-15'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-12-01', 'end_date': '2019-12-08'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-11-24', 'end_date': '2019-12-01'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-11-17', 'end_date': '2019-11-24'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-11-10', 'end_date': '2019-11-17'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-11-03', 'end_date': '2019-11-10'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-10-27', 'end_date': '2019-11-03'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-10-20', 'end_date': '2019-10-27'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-10-13', 'end_date': '2019-10-20'
#     },
#     {
#         'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
#         'max_freq': 14, 'start_date': '2019-10-06', 'end_date': '2019-10-13'
#     },
# ]

# generate_freq_dist_series = [
#     {
#         'company_id': 1826, 'shipped_method': 'DHL Parcel International Standard',
#         'date_range_type': 'month', 'max_freq': 30,
#         'start_date': '2019-09-01', 'end_date': '2019-10-01'
#     },
#     {
#         'company_id': 1826, 'shipped_method': 'DHL Parcel International Standard',
#         'date_range_type': 'month', 'max_freq': 30,
#         'start_date': '2019-10-01', 'end_date': '2019-11-01'
#     },
#     {
#         'company_id': 1826, 'shipped_method': 'DHL Parcel International Standard',
#         'date_range_type': 'month', 'max_freq': 30,
#         'start_date': '2019-11-01', 'end_date': '2019-12-01'
#     },
#     {
#         'company_id': 1826, 'shipped_method': 'DHL Parcel International Standard',
#         'date_range_type': 'month', 'max_freq': 30,
#         'start_date': '2019-12-01', 'end_date': '2020-01-01'
#     },
# ]


####################################################################################################

####################################################################################################



"""

NOTES:
- Set ['start_date'] to empty string for auto generating most recent entry to pull.
- ['start_date'] should be empty for automated data pull.  If ['start_date'] is empty then TOTALS
  report will accumulate as well as individuals.  If ['start_date'] is not empty, then TOTALS
  report will not accumulate and be ignored.
- (for now) ONLY set ['max_freq'] to 14!

"""

weekly_accumulation_report_series = [
    {
        'company_id': 507, 'shipped_method': 'USPS Media Mail', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '2020-01-05'
    },
    {
        'company_id': 735, 'shipped_method': 'USPS Media Mail', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '2020-01-05'
    },
    {
        'company_id': 1232, 'shipped_method': 'USPS Media Mail', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '2020-01-05'
    },
    {
        'company_id': 1899, 'shipped_method': 'UPS MI BPM', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '2020-01-05'
    },
    {
        'company_id': 1584, 'shipped_method': 'UPS MI Dom', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '2020-01-05'
    },
    {
        'company_id': 1236, 'shipped_method': 'UPS MI Dom', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '2020-01-05'
    },
    {
        'company_id': 752, 'shipped_method': 'UPS MI Dom', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '2020-01-05'
    },
    {
        'company_id': 816, 'shipped_method': 'USPS Priority', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '2020-01-05'
    },
    {
        'company_id': 1899, 'shipped_method': 'UPS MI Parcel Select', 'date_range_type': 'week',
        'max_freq': 14, 'start_date': '2020-01-05'
    },
]
