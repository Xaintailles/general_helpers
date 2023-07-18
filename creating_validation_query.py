# USER INPUT #

# all columns, in a list format
list_all_columns = [
  'order_id',
  'unique_transaction_id',
  'report_date',
  'marketing_group_id',       
  'marketing_subgroup_id',    
  'marketing_channel_id',     
  'marketing_subchannel_id',  
  'marketing_category_id',    
  'marketing_subcategory_id', 
  'marketing_campaign_id',    
  'distributed_cost',         
  'platform',                 
  'country',                  
]

# table that contains the data you are comparing to
rs_table = '`example_table`'

# table that contains the data that you are testing
bq_table = '`example_table_2`'

# key on which to join the two tables, still have to work on getting multiple primary keys
primary_keys = [
    'order_id',
]

# END OF USER INPUT #

if len(primary_keys) == 1:
    join_keys = f'ON rs.{primary_keys[0]} = bq.{primary_keys[0]}'
else:
    for i in range(0,len(primary_keys)):
        if i == 0:
            join_keys = f'ON rs.{primary_keys[i]} = bq.{primary_keys[i]}\n'
        else:
            join_keys = join_keys + f'AND rs.{primary_keys[i]} = bq.{primary_keys[i]}\n'

all_columns_names = []

for col_name in list_all_columns:
    all_columns_names.append(f'matching_{col_name},\n')
    all_columns_names.append(f'non_matching_{col_name},\n')
    all_columns_names.append(f'nulls_{col_name},\n')
    all_columns_names.append(f'total_count_{col_name},\n')

all_columns_string = ''.join(all_columns_names)

all_checks_list = []

for col_name in list_all_columns:
    all_checks_list.append(f"SUM(CASE WHEN CAST(rs.{col_name} AS STRING) = CAST(bq.{col_name} AS STRING) THEN 1 ELSE 0 END) AS matching_{col_name},\n")
    all_checks_list.append(f"SUM(CASE WHEN CAST(rs.{col_name} AS STRING) <> CAST(bq.{col_name} AS STRING) THEN 1 ELSE 0 END) AS non_matching_{col_name},\n")
    all_checks_list.append(f"SUM(CASE WHEN CAST(rs.{col_name} AS STRING) IS NULL OR CAST(bq.{col_name} AS STRING) IS NULL THEN 1 ELSE 0 END) AS nulls_{col_name},\n")
    all_checks_list.append(f"SUM(CASE WHEN CAST(rs.{col_name} AS STRING) = CAST(bq.{col_name} AS STRING) OR CAST(rs.{col_name} AS STRING) IS NULL OR CAST(bq.{col_name} AS STRING)IS NULL THEN 1 ELSE 0 END) AS total_count_{col_name},\n")


all_checks = ''.join(all_checks_list)

query = '''WITH all_validation AS (
    SELECT\n''' + all_checks + f'''COUNT(*) AS total_count
FROM {rs_table} rs
INNER JOIN {bq_table} bq
''' + join_keys + f''')
SELECT * FROM all_validation
UNPIVOT(value FOR category IN ({all_columns_string}total_count))
'''

print(query)
