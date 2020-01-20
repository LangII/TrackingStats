
"""

"""

####################################################################################################

update_tracking_series = [
    {'company_id': 507,     'shipped_method': 'USPS Media Mail',    'days_ago': 30},
    {'company_id': 735,     'shipped_method': 'USPS Media Mail',    'days_ago': 30},
    {'company_id': 1232,    'shipped_method': 'USPS Media Mail',    'days_ago': 30},
    {'company_id': 1899,    'shipped_method': 'UPS MI BPM',         'days_ago': 30},
    {'company_id': 1584,    'shipped_method': 'UPS MI Dom',         'days_ago': 30},
    {'company_id': 1236,    'shipped_method': 'UPS MI Dom',         'days_ago': 30},
    {'company_id': 752,     'shipped_method': 'UPS MI Dom',         'days_ago': 30},
    {'company_id': 816,     'shipped_method': 'USPS Priority',      'days_ago': 30}
]

# Series notes:
#  - If ['start_date'] and ['end_date'] are empty, script will auto fill with most recent Sundays.
#  - ['date_range_type'] has to be 'day', 'week', 'month', or 'custom'.
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
    }
]
