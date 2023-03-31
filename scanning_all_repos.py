# -*- coding: utf-8 -*-
"""
Created on Fri Mar 31 17:15:03 2023

@author: Gebruiker
"""

from os import walk
from os.path import isfile
import pandas as pd

all_repos = [
    'da-etl-restaurants',
    'da-etl-marketing',
    'da-etl-customer-service',
    'da-etl-human-resources',
    'da-etl-finance',
    'airflow-dags',
    'de-dwh-dags',
]

all_keywords = [
'point_id',
'affected_material_id',
'vendor_id',
'incident_id',
'app_engagement_message_id',
'worker_type_id',
'company_id',
'department_id',
'management_level_id',
'order_action_id',
'driver_absence_status_type_id',
'SitelanguageId',
'transaction_type_id',
'polygon_id',
'campaign_id',
'shift_classification_type_id',
'driver_absence_type_id',
'disciplinary_entry_type_id',
'refund_reason_id',
'order_fraud_status_id',
'user_id',
'orderid',
'loyalty_partner_id',
'app_engagement_app_id',
'app_engagement_event_id',
'app_engagement_channel_id',
'platform_id',
'app_engagement_true_session_id',
'app_engagement_device_id',
'applicant_id',
'loyalty_offer_id',
'loyalty_point_rule_id',
'loyalty_redemption_id',
'standardisation_process_id',
'standardisation_responder_position_id',
'shift_type_id',
'domainid',
'rowid',
'transaction_id',
'disciplinary_entry_id',
'eventid',
'refund_id',
'offer_id',
'voucher_id',
'employee_id',
'option_id',
'customer_id',
'option_group_id',
'scoober_pool_job_id',
'exchangerateid',
'product_id',
'submission_id',
'user_id',
    ]

df_keywords = pd.DataFrame(all_keywords, columns = ['keyword'])
df_keywords['key'] = 0

root_path = r'Users/calixte.allier/'

paths = []

for repo in all_repos:
    for (dirpath, dirnames, filenames) in walk(repo):
        for file in filenames:
            file_to_test = str(dirpath) + r'/' + str(file)
            if isfile(file_to_test) and file.find('sql') >= 0:
                paths.append(file_to_test)

all_queries = []
all_paths = []
total_len = len(paths)
i = 1

for file_path in paths:
    if i%500 == 0 or i == total_len:
        print(f'opening file {i} out of {total_len}')
    i = i + 1
    with open(file_path,'r') as f:
        try:
            all_queries.append(f.read())
            all_paths.append(file_path)
        except:
            continue

print('starting to process dataframe')

df = pd.DataFrame(zip(all_paths,all_queries), columns = ['path','query'])
df['key'] = 0

df = df.merge(df_keywords, on='key')

df['keyword_match'] = df.apply(lambda x: x.keyword in x.query, axis = 1)

matches_df = df[df['keyword_match'] == True]

matches_df = matches_df.filter(['path','keyword'])

matches_df.to_csv('all_files.csv')

print('success saving to all_files.csv')