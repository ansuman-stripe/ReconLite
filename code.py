# Importing required packages
import os
from autohubble import hubble_query_to_df_and_permalink, PRESTO
import pandas as pd
import numpy as np
import csv
import re
from datetime import datetime

# Directory where your files are stored
directory_path = '/Users/ansuman/Downloads'
script_directory = os.path.dirname(__file__)
date_pattern = re.compile(r'Missing VBANs Report (\d{8}) to (\d{8})')

def extract_dates(filename):
    match = date_pattern.search(filename)
    if match:
        start_date_str, end_date_str = match.groups()
        start_date = datetime.strptime(start_date_str, "%m%d%Y")
        end_date = datetime.strptime(end_date_str, "%m%d%Y")
        return start_date, end_date
    return None


# List and filter files based on the date pattern
files = os.listdir(directory_path)
files_with_dates = []
for file in files:
    dates = extract_dates(file)
    if dates:
        start_date, end_date = dates
        files_with_dates.append((file, start_date, end_date))

if not files_with_dates:
    print("No files matching the pattern were found.")
else:
    files_with_dates.sort(key=lambda x: x[2], reverse=True)
    latest_file = files_with_dates[0][0]
    latest_file_path = os.path.join(directory_path, latest_file)

    try:
        # Read the latest file
        data = pd.read_csv(latest_file_path, header=3, encoding='ISO-8859-1')

        # Validate column names and extract data
        selected_columns = data[["Msg Dr Sbk Ref Num", "WPIC Account"]]
        
        # Path to the main CSV file
        main_csv_path = os.path.join(script_directory, 'main_data.csv')

        # Load existing data from the main CSV file
        if os.path.exists(main_csv_path):
            main_data = pd.read_csv(main_csv_path)
        else:
            main_data = pd.DataFrame(columns=selected_columns.columns)

        # Concatenate the new data and remove duplicates
        combined_data = pd.concat([main_data, selected_columns]).drop_duplicates()
        
        # Save the updated main data back to the CSV
        combined_data.to_csv(main_csv_path, index=False)
        print(f"Updated main data saved to {main_csv_path}")
        
    except UnicodeDecodeError as e:
        print(f"Encoding error: {e}")
    except KeyError as e:
        print(f"Column error: {e}")
    except pd.errors.ParserError as e:
        print(f"Parsing error: {e}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

main_csv_path = os.path.join(script_directory, 'main_data.csv')
main_data = pd.read_csv(main_csv_path, dtype=str)

################################################################################################ Creating master data table for processing
# Hubble query to pull incident pbats
sql_pbat_data = f'''
with pbat_cte as (
    select p.Date,_id as pbat, bank_Account_transaction, p.amount / 100.00 as amount, p.description as pdescription, i.description as idescription, cast(regexp_extract(p.description, 'WT ([0-9]+)', 1) as varchar) as ref, i.ibat
  from mongo.parsedbankaccounttransactions p
  join ( 
    select _id as ibat,date, amount, account, description 
    from mongo.intradaybankaccounttransactions where description like '%BNF=STRIPE%'
  ) as i on i.description like concat('%', regexp_extract(p.description, 'WT ([0-9]+)', 1), '%')
  where 1=1
  and reconciliation_key is null
  and length(regexp_extract(p.description, 'WT ([0-9]+)', 1)) = 16
  and label = 'wells_ach_perfect_receivable'
  and currency = 'usd'
  order by date desc
  set session default_locale_only=true
), jira_cte as (
  select issue_link,description as jdescription from jiradb.denormalized_jiraissue
  where 1=1
  and project_id = 42801
  and (labels like '%ach-perfectreceivable%' and labels like '%sdc-techops-bulk-clearing%' and labels like '%sdc-techops-projects%' and labels like '%sdc-ui-clearing%')
) select Date, pbat, bank_Account_transaction, amount, pdescription, idescription, ref, ibat, issue_link 
from pbat_cte left join jira_cte on jdescription like concat('%',pbat,'%')
where issue_link is null
'''
permalink_pbat_data, df_pbat_data = hubble_query_to_df_and_permalink(sql_pbat_data, PRESTO)

# Perform a left join on 'Msg Dr Sbk Ref Num' from main_data with 'ref' in df_pbat_data
merged_vban_data = pd.merge(df_pbat_data.astype(str),
    combined_data.astype(str).rename(columns={'Msg Dr Sbk Ref Num': 'ref'}),  # Rename for join consistency
    on='ref',how='left')

# Rename 'WPIC Account' column to 'vban' and filling null values
merged_vban_data.rename(columns={'WPIC Account': 'VBAN'}, inplace=True)
merged_vban_data['VBAN']= merged_vban_data['VBAN'].fillna('VBAN Not provided by WF')

# Pulling valid vbans to get account and cu object
valid_vbans = merged_vban_data[merged_vban_data['VBAN'] != 'VBAN Not provided by WF']['VBAN'].values
vbans_output = '|'.join(valid_vbans)

sql_cu_data = f'''
 WITH base AS (
  select
    vban.state AS vban_state,
    vban.type,
  concat_ws(
      '::',
      vban.vban.ach.account_number,
      vban.vban.indonesia_ct.account_number,
      vban.vban.japan_bt.account_number,
      vban.vban.sepa.account_number,
      vban.vban.sepa.iban,
      vban.vban.spei.clabe,
      vban.vban.uk_ct.account_number
    ) AS account_number,
    vamv.funding_flow.destination_customer_balance.customer,
    vamv.merchant,
    from_unixtime(vamv.obj_attr.c_time.millis / 1000) AS vban_allocated_on,
    vban.id AS vr_id,
    vnmv.id AS vnm_id
  from
    iceberg.vbandb.vban_record vban
    left join iceberg.h_merchant_banktransfersfpi.sharded_vban_network_model_records vnmv ON vban.external_id = vnmv.id
    left join iceberg.h_merchant_banktransfersfpi.sharded_vban_allocation_model_records vamv ON vnmv.vban_allocation_id = vamv.id
)
select
  account_number,customer,merchant
from
  base b
where 1=1
and type = 'WELLS_FARGO_USD'
and vban_state = 'ALLOCATED'
and regexp_like(account_number, '{vbans_output}')
'''
permalink_cu_data, df_cu_data = hubble_query_to_df_and_permalink(sql_cu_data, PRESTO)

# Perform a left join on 'VBAN' from merged_vban_data with 'account_number' in df_cu_data
merged_cu_data = pd.merge(merged_vban_data.astype(str),
    df_cu_data.astype(str).rename(columns={'account_number': 'VBAN','customer': 'cu_src_object' }),  # Rename for join consistency
    on='VBAN',how='left')

# Pulling valid vbans to get account and src object
filtered_vbans = merged_cu_data[(merged_cu_data['cu_src_object'].isnull()) &(merged_cu_data['merchant'].isnull()) &(merged_cu_data['VBAN'] != 'VBAN Not provided by WF')]['VBAN'].values
vbans_output = ','.join(f"'{vban}'" for vban in filtered_vbans)

sql_src_data = f'''
select _id as cu_src_object,merchant,cast(json_extract(external_data, '$.account_number') AS varchar) as VBAN,status as src_status
from mongo.sources
where 1=1
and cast(json_extract(external_data, '$.account_number') AS varchar) in ({vbans_output}) 
'''
permalink_src_data, df_src_data = hubble_query_to_df_and_permalink(sql_src_data, PRESTO)

# Perform a left join on 'VBAN' from merged_cu_data with 'account_number' in sql_src_data
merged_final_data = pd.merge(merged_cu_data.astype(str),df_src_data.astype(str),on='VBAN',how='left')

merged_final_data['cu_src_object'] = np.where(
    merged_final_data['cu_src_object_y'].notnull(),merged_final_data['cu_src_object_y'],merged_final_data['cu_src_object_x'])
merged_final_data['merchant'] = np.where(
    merged_final_data['merchant_y'].notnull(),merged_final_data['merchant_y'],merged_final_data['merchant_x'])
merged_final_data.drop(columns=['cu_src_object_x', 'cu_src_object_y', 'merchant_x', 'merchant_y'], inplace=True)

merged_final_data['src_status'] = np.where(
    (merged_final_data['cu_src_object'].str.startswith('cu_')) & 
    (merged_final_data['VBAN'] != 'VBAN Not provided by WF'),
    'Horizon',
    merged_final_data['src_status']  # Leave original status (which might be NaN) untouched otherwise
)

merged_final_data['merchant'] = merged_final_data['merchant'].replace('nan', np.nan)
valid_acct = merged_final_data[merged_final_data['merchant'].notnull()]['merchant'].values
acct_output = ','.join(f"'{acct}'" for acct in valid_acct)

sql_acct_data = f'''
select merchant_id as merchant,account_applications__latest__application_state as merchant_status,is_rejected,is_deleted from cdm.merchants_core
where 1=1
and merchant_id in ({acct_output})
'''
permalink_acct_data, df_acct_data = hubble_query_to_df_and_permalink(sql_acct_data, PRESTO)

# Perform a left join on 'VBAN' from merged_cu_data with 'account_number' in sql_src_data
merged_master_data = pd.merge(merged_final_data.astype(str),df_acct_data.astype(str),on='merchant',how='left')

# Save the merged data with the required columns
merged_csv_path = os.path.join(script_directory, 'merged_data.csv')
merged_master_data.to_csv(merged_csv_path, index=False)
print(f"\nMerged data saved to {merged_csv_path}")

################################################################################################ Segregating data as per processing rails
# Data for Generate Synthtic IBAT
filtered_data_for_cu_excelsior = merged_master_data[
    (merged_master_data['src_status'] == 'Horizon') & 
    (merged_master_data['merchant_status'] != 'rejected') & 
    (merged_master_data['is_deleted'] == 'False') & 
    (merged_master_data['is_rejected'] == 'False')
]

# Exclusion: Remove rows from merged_master_data for next operation
merged_master_data = merged_master_data[~merged_master_data.index.isin(filtered_data_for_cu_excelsior.index)]

# Data for Manually Update Wire description
filtered_data_for_src_excelsior = merged_master_data[
    ((merged_master_data['src_status'] == 'pending') | (merged_master_data['src_status'] == 'chargeable') ) & 
    (merged_master_data['merchant_status'] != 'rejected') & 
    (merged_master_data['is_deleted'] == 'False') & 
    (merged_master_data['is_rejected'] == 'False')
]

# Data to upload to Jira duing creation
jira_upload_data = pd.concat([filtered_data_for_cu_excelsior, filtered_data_for_src_excelsior])

# Save the Jira data with the required columns
jira_upload_path = os.path.join(script_directory, 'jira_upload_data.csv')
jira_upload_data.to_csv(jira_upload_path, index=False)
print(f"\nJira data saved to {jira_upload_path}")

# Exclusion: Remove rows from merged_master_data for next operation
merged_master_data = merged_master_data[~merged_master_data.index.isin(filtered_data_for_src_excelsior.index)]

# Save the Exception Cases data with the required columns
exceptionCases_csv_path = os.path.join(script_directory, 'exceptionCases_data.csv')
merged_master_data.to_csv(exceptionCases_csv_path, index=False)

################################################################################################ Processing Data for excelsior task
# Required columns to run Generate Synthetic IBAT excelsior
jira_link = input("Please enter the Jira link: ")
required_columns = ['pbat', 'VBAN']
filtered_data_for_cu_excelsior = filtered_data_for_cu_excelsior[required_columns]
filtered_data_for_cu_excelsior.rename(columns={'pbat': 'pbat_ids', 'VBAN': 'vban_account_number'}, inplace=True)

# Add new columns with specified default values
filtered_data_for_cu_excelsior['storytime'] = jira_link if jira_link else None
filtered_data_for_cu_excelsior['prepend_wire_reference'] = 'TRUE'
filtered_data_for_cu_excelsior['always_override_vban_account_number'] = 'FALSE'
filtered_data_for_cu_excelsior['partner'] = 'wellsfargo'

# Reorder columns to match the desired output order
output_order = ['pbat_ids', 'storytime', 'prepend_wire_reference', 'vban_account_number', 
                'always_override_vban_account_number', 'partner']
filtered_data_for_cu_excelsior = filtered_data_for_cu_excelsior[output_order]

# Save the merged data with the required columns
generateSynthticIBAT_csv_path = os.path.join(script_directory, 'generateSynthticIBAT_data.csv')
# merged_data[required_columns].to_csv(merged_csv_path, index=False)
filtered_data_for_cu_excelsior.to_csv(generateSynthticIBAT_csv_path, index=False)

# Required columns
required_columns = ['pbat','ibat', 'cu_src_object']
filtered_data_for_src_excelsior = filtered_data_for_src_excelsior[required_columns]
filtered_data_for_src_excelsior.rename(columns={'cu_src_object': 'source'}, inplace=True)

# Add new columns with specified default values
filtered_data_for_src_excelsior['prepend_wire_reference'] = 'FALSE'
filtered_data_for_src_excelsior['append_bnf'] = 'TRUE'

# Reorder columns to match the desired output order
output_order = ['pbat', 'ibat', 'source', 'prepend_wire_reference', 
                'append_bnf']
filtered_data_for_src_excelsior = filtered_data_for_src_excelsior[output_order]

# Save the merged data with the required columns
manuallyUpdateWireDescription_csv_path = os.path.join(script_directory, 'manuallyUpdateWireDescription_data.csv')
filtered_data_for_src_excelsior.to_csv(manuallyUpdateWireDescription_csv_path, index=False)