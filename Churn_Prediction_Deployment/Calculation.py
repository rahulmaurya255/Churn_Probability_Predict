import pandas as pd
import numpy as np
from datetime import datetime
import pandas as pd
import hashlib
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)
#Self Libraries
from DB.DBConstants import *
from Functions.time_stamp import *
from Functions.helper_functions import *
logger.debug(f"Imported all libraries")
logger.debug(f"{time_period}")
logger.debug(f"Ride SQL queries")


#person_table

new_riders_query = """
            select id as rider_id, first_name, gender,created_at as registered_on, merchant_operating_city_id as mocid, merchant_id as mid
            from atlas_app.person final
            where 
            registered_on > '2024-11-18 18:30:00'     --- take 40 days of data from rider table
"""

new_bookings_query = """
            select rider_id, id as booking_id, created_at as booking_created_at, status as booking_status,from_location_id,to_location_id
            from atlas_app.booking
            where created_at > '2024-11-18 18:30:00'
"""

booking_cancellations_query = """
            SELECT b.booking_id as booking_id, c.source
FROM atlas_app.booking_cancellation_reason c
JOIN 
  (SELECT id AS booking_id, created_at
   FROM atlas_app.booking
   WHERE created_at > '2024-12-01 18:30:00') b 
ON c.booking_id = b.booking_id
WHERE b.created_at > '2024-12-01 18:30:00'
"""



ride_query = """
        select booking_id, vehicle_variant, total_fare, chargeable_distance
        from atlas_app.ride
        where
        status = 'COMPLETED'
        and created_at > '2024-11-18 18:30:00'
"""

bap_search_request_query = """
            select rider_id , id as bap_sr_id, created_at
            from atlas_app.search_request
            where
            created_at > '2024-11-18 18:30:00'
"""


bpp_search_try_query = """
        select id as bpp_st_id, request_id as bpp_sr_id, created_at, search_repeat_type
        from atlas_driver_offer_bpp.search_try
        created_at > '2024-11-18 18:30:00'
"""

bpp_search_request_query = """
        select id as bpp_sr_id, transaction_id as bap_sr_id
        from atlas_driver_offer_bpp.search_request
        created_at > '2024-11-18 18:30:00'
"""


new_riders = clickhouse_connection(new_riders_query)
new_bookings = clickhouse_connection(new_bookings_query)
booking_cancellations = clickhouse_connection(booking_cancellations_query)
ride = clickhouse_connection(ride_query)
bap_search_request = clickhouse_connection(bap_search_request_query)
bpp_search_try = clickhouse_connection(bpp_search_try_query)
bpp_search_request = clickhouse_connection(bpp_search_request_query)




# ###### testing
# new_riders = pd.read_csv('/Users/rahulmaurya/Documents/Churn Prediction/test csv/Input csv/New_riders.csv')
# new_bookings = pd.read_csv('/Users/rahulmaurya/Documents/Churn Prediction/test csv/Input csv/new_bookings.csv')
# booking_cancellations = pd.read_csv('/Users/rahulmaurya/Documents/Churn Prediction/test csv/Input csv/Booking_cancellationn.csv')
# ride = pd.read_csv('/Users/rahulmaurya/Documents/Churn Prediction/test csv/Input csv/ride.csv')
# bap_search_request = pd.read_csv('/Users/rahulmaurya/Documents/Churn Prediction/test csv/Input csv/bap_search_request.csv')

All_new_riders_df = pd.merge(new_riders,bap_search_request,on='rider_id',how='outer')
All_new_riders_df = All_new_riders_df[['rider_id']]


##calculating first search n last search date (df = Searches_date_n_count)
bap_search_request['created_at'] = pd.to_datetime(bap_search_request['created_at'])

Searches_date_n_count = bap_search_request.groupby('rider_id').agg(
    first_search_date=('created_at', 'min'),
    last_search_date=('created_at', 'max'),
    total_searches=('bap_sr_id', 'nunique')
).reset_index()

# ## way to merge 
# sr_m1 = pd.merge(sr1,sr2,on='rider_id',how='outer',suffixes=('03_2024','_07_2024'))

# sr_m1['first_search_date'] = sr_m1[['first_search_date03_2024','first_search_date_07_2024']].min(axis=1)


#calculating bookings (df = total_bookings)

new_bookings['booking_created_at'] = pd.to_datetime(new_bookings['booking_created_at'])

total_bookings = new_bookings.groupby('rider_id')['booking_id'].nunique().reset_index()
total_bookings.columns = ['rider_id', 'total_bookings']

# # first_booking_date, last_booking_date , gap in first n last booking  (df = Gap_in_bookings)

Gap_in_bookings = new_bookings.groupby('rider_id').agg(
    first_booking_date =  ('booking_created_at','min'),
    last_booking_date = ('booking_created_at','max')
).reset_index()

## ------ calculate Gap in first and last booking after merging csvs



#booking cancellation source 
rider_id_booking_id = new_bookings[['rider_id','booking_id']]

bcr_rider_id = pd.merge(rider_id_booking_id, booking_cancellations, how= 'left', on='booking_id')

By_driver = bcr_rider_id[bcr_rider_id['source']=='ByDriver']
ByUser = bcr_rider_id[bcr_rider_id['source']=='ByUser']
ByMerchant_or_app = bcr_rider_id[(bcr_rider_id['source']=='ByMerchant') | (bcr_rider_id['source']=='ByApplication')]

# Calculate counts by rider_id for each Sources   (df= Canc)

By_driver_count = By_driver.groupby('rider_id')['booking_id'].nunique().reset_index()
By_driver_count.columns = ['rider_id','ByDriverCanc']

ByUser_count = ByUser.groupby('rider_id')['booking_id'].nunique().reset_index()
ByUser_count.columns = ['rider_id','ByUserCanc']

ByMerchant_or_app_count = ByMerchant_or_app.groupby('rider_id')['booking_id'].nunique().reset_index()
ByMerchant_or_app_count.columns = ['rider_id','ByAppOrMerchant']

# Merge the counts into the 'Canc' dataframe, using 'rider_id' as the key
Canc1 = pd.merge(By_driver_count,ByUser_count ,on='rider_id', how='outer')
Canc = pd.merge( Canc1,ByMerchant_or_app_count,on='rider_id', how='outer')
Canc.fillna(0,inplace=True)

# Convert the counts to integers if you want the result as integer values (optional)
Canc['ByDriverCanc'] = Canc['ByDriverCanc'].astype(int)
Canc['ByUserCanc'] = Canc['ByUserCanc'].astype(int)
Canc['ByAppOrMerchant'] = Canc['ByAppOrMerchant'].astype(int)



##completed bookings / rides (df = First_last_ride)

ride_df  = new_bookings[new_bookings['booking_status']!='COMPLETED']

Completed_rides = ride_df.groupby('rider_id')['booking_id'].nunique().reset_index()
Completed_rides.columns = ['rider_id', 'total_rides_overall']

First_last_ride = ride_df.groupby('rider_id').agg(
    first_ride_date =  ('booking_created_at','min'),
    last_ride_date = ('booking_created_at','max')
).reset_index()



###rides in first_30 days  (df = 'Rides_in_first_30_days')

reg_date = new_riders[['rider_id','registered_on']]
ride_df_reg_date  = pd.merge(ride_df,reg_date,on='rider_id',how='left')

ride_df_reg_date['gap_in_rides'] = (pd.to_datetime(ride_df_reg_date['booking_created_at']) - pd.to_datetime(ride_df_reg_date['registered_on'])).dt.days

# Optionally, fill NaN values with a default value (e.g., 0)
ride_df_reg_date['gap_in_rides'] = ride_df_reg_date['gap_in_rides'].fillna(0).astype(int)
ride_df_reg_date.drop(columns=['booking_created_at','registered_on'],inplace=True)

ride_df_reg_date = ride_df_reg_date[ride_df_reg_date['gap_in_rides']<=30]
Rides_in_first_30_days =ride_df_reg_date.groupby('rider_id')['booking_id'].nunique().reset_index()
Rides_in_first_30_days.columns = ['rider_id','Rides_in_first_30_days']




####overall total rides and distance travelled (df = rides_n_distance )

rides_n_distance = pd.merge(ride,rider_id_booking_id,on='booking_id',how='left')

Total_fare_n_dist_travelled = rides_n_distance.groupby('rider_id').agg(
    total_rides_overall = ('booking_id','count'),
    total_dist_travelled_overall = ('chargeable_distance','sum'),
    total_fair_paid = ('total_fare','sum')
).reset_index()



### rides , distance n fair varint wise
ride = pd.merge(ride,rider_id_booking_id,on='booking_id',how='left')
# Grouping the DataFrame by 'vehicle_variant' and storing in a dictionary
vehicle_variant_dfs = {variant: group for variant, group in ride.groupby('vehicle_variant')}

# Accessing individual DataFrames, for example:
auto_rickshaw_df = vehicle_variant_dfs['AUTO_RICKSHAW']
taxi_df = vehicle_variant_dfs['TAXI']
hatchback_df = vehicle_variant_dfs['HATCHBACK']
sedan_df = vehicle_variant_dfs['SEDAN']
suv_df = vehicle_variant_dfs['SUV']             #changing to test
bike_df = vehicle_variant_dfs['AUTO_RICKSHAW']          #changing to test
delivery_bike_df = vehicle_variant_dfs['AUTO_RICKSHAW']   #changing to test
suv_plus_df = vehicle_variant_dfs['AUTO_RICKSHAW']
taxi_plus_df = vehicle_variant_dfs['AUTO_RICKSHAW']


Rides_n_fare_Auto = auto_rickshaw_df.groupby('rider_id').agg(
    total_no_of_auto_rides=('booking_id', 'count'),
    total_dist_travelled_in_auto =('chargeable_distance', 'sum'),
    total_fair_paid_in_auto=('total_fare', 'sum')
).reset_index()


Rides_n_fare_taxi_df = taxi_df.groupby('rider_id').agg(
    total_no_of_taxi_rides=('booking_id', 'count'),
    total_dist_travelled_in_taxi =('chargeable_distance', 'sum'),
    total_fair_paid_in_taxi=('total_fare', 'sum')
).reset_index()

Rides_n_fare_hatchback_df = hatchback_df.groupby('rider_id').agg(
    total_no_of_hatchback_rides=('booking_id', 'count'),
    total_dist_travelled_in_hatchback=('chargeable_distance', 'sum'),
    total_fair_paid_in_hatchback=('total_fare', 'sum')
).reset_index()

Rides_n_fare_sedan_df = sedan_df.groupby('rider_id').agg(
    total_no_of_sedan_rides=('booking_id', 'count'),
    total_dist_travelled_in_sedan=('chargeable_distance', 'sum'),
    total_fair_paid_in_sedan=('total_fare', 'sum')
).reset_index()

Rides_n_fare_suv_df = suv_df.groupby('rider_id').agg(
    total_no_of_suv_rides=('booking_id', 'count'),
    total_dist_travelled_in_suv=('chargeable_distance', 'sum'),
    total_fair_paid_in_suv=('total_fare', 'sum')
).reset_index()

Rides_n_fare_bike_df = bike_df.groupby('rider_id').agg(
    total_no_of_bike_rides=('booking_id', 'count'),
    total_dist_travelled_in_bike=('chargeable_distance', 'sum'),
    total_fair_paid_in_bike=('total_fare', 'sum')
).reset_index()

Rides_n_fare_delivery_bike_df = delivery_bike_df.groupby('rider_id').agg(
    total_no_of_delivery_bike_rides=('booking_id', 'count'),
    total_dist_travelled_in_delivery_bike =('chargeable_distance', 'sum'),
    total_fair_paid_in_delivery_bike=('total_fare', 'sum')
).reset_index()

Rides_n_fare_suv_plus_df = suv_plus_df.groupby('rider_id').agg(
    total_no_of_suv_plus_rides=('booking_id', 'count'),
    total_dist_travelled_in_suv_plus=('chargeable_distance', 'sum'),
    total_fair_paid_in_suv_plus=('total_fare', 'sum')
).reset_index()

Rides_n_fare_taxi_plus_df = taxi_plus_df.groupby('rider_id').agg(
    total_no_of_taxi_plus_rides=('booking_id', 'count'),
    total_dist_travelled_in_taxi_plus=('chargeable_distance', 'sum'),
    total_fair_paid_in_taxi_plus=('total_fare', 'sum')
).reset_index()





# Start with the main DataFrame
final_df = All_new_riders_df

# Merge with Searches_date_n_count (df = Searches_date_n_count)
final_df = pd.merge(final_df, Searches_date_n_count, on='rider_id', how='left')

# Merge with Gap_in_bookings (df = Gap_in_bookings)
final_df = pd.merge(final_df, Gap_in_bookings, on='rider_id', how='left')

# Merge with Canc (df = Canc)
final_df = pd.merge(final_df, Canc, on='rider_id', how='left')

# Merge with First_last_ride (df = First_last_ride)
final_df = pd.merge(final_df, First_last_ride, on='rider_id', how='left')

# Merge with Rides_in_first_30_days (df = Rides_in_first_30_days)
final_df = pd.merge(final_df, Rides_in_first_30_days, on='rider_id', how='left')

# Merge with rides_n_distance (df = rides_n_distance)
final_df = pd.merge(final_df, rides_n_distance, on='rider_id', how='left')

# Merge with vehicle variant data (e.g., 'auto', 'bike', 'delivery_bike', etc.)
vehicle_variant_dfs = {
    'auto': Rides_n_fare_Auto,
    'bike': Rides_n_fare_bike_df,
    'delivery_bike': Rides_n_fare_delivery_bike_df,
    'hatchback': Rides_n_fare_hatchback_df,
    'sedan': Rides_n_fare_sedan_df,
    'suv': Rides_n_fare_suv_df,
    'suv_plus': Rides_n_fare_suv_plus_df,
    'taxi': Rides_n_fare_taxi_df,
    'taxi_plus': Rides_n_fare_taxi_plus_df
}

# Merging each vehicle variant DataFrame
for variant, df in vehicle_variant_dfs.items():
    final_df = pd.merge(final_df, df, on='rider_id', how='left')

# Fill NaN values (if any) with 0
final_df.fillna(0, inplace=True)



# Calculate days since registration
final_df['days_since_reg'] = (datetime.now() - pd.to_datetime(final_df['registered_on'])).dt.days


final_df.to_csv('/Users/rahulmaurya/Documents/Churn Prediction/test csv/Outputs csv/output_f.csv',index=False)




### after Calculating final csv , 



## rider_ids 
rider_ids_to_update = final_df['rider_id'].tolist()
rider_ids_to_update = ', '.join(f"'{rider_id}'" for rider_id in rider_ids_to_update)
# rider_ids_to_update = f"({rider_ids_to_update})"



# to update the rows in table take  dump from person_meta for related ids only

df1 = f"""
    select * from atlas_app.person_meta
    where rider_id in ({rider_ids_to_update})
""" ### ----- don't take columns  total_gap_in_first_n_last_booking, churn_status, probability

#### Merging

# First, merge the DataFrames on 'rider_id' using an outer join
merged_df = pd.merge(df1, final_df, on='rider_id', suffixes=('_df1', '_final_df'))

# Define the aggregation functions for each column
aggregations = {
    'first_search_date': 'min',
    'last_search_date': 'max',
    'total_searches': 'sum',
    'first_ride_date': 'min',
    'last_ride_date': 'max',
    'first_booking_date': 'min',
    'last_booking_date': 'max',
    'rides_in_first_30_days': 'max',  #  will handle this separately
    'total_bookings': 'sum',
    'ByDriverCanc': 'sum',
    'ByUserCanc': 'sum',
    'ByAppOrMerchantCanc': 'sum',
    'total_rides_overall': 'sum',
    'total_dist_travelled_overall': 'sum',
    'total_fair_paid': 'sum',
    'total_no_of_auto_rides': 'sum',
    'total_dist_travelled_in_auto': 'sum',
    'total_fair_paid_in_auto': 'sum',
    'total_no_of_bike_rides': 'sum',
    'total_dist_travelled_in_bike': 'sum',
    'total_fair_paid_in_bike': 'sum',
    'total_no_of_delivery_bike_rides': 'sum',
    'total_dist_travelled_in_delivery_bike': 'sum',
    'total_fair_paid_in_delivery_bike': 'sum',
    'total_no_of_hatchback_rides': 'sum',
    'total_dist_travelled_in_hatchback': 'sum',
    'total_fair_paid_in_hatchback': 'sum',
    'total_no_of_sedan_rides': 'sum',
    'total_dist_travelled_in_sedan': 'sum',
    'total_fair_paid_in_sedan': 'sum',
    'total_no_of_suv_rides': 'sum',
    'total_dist_travelled_in_suv': 'sum',
    'total_fair_paid_in_suv': 'sum',
    'total_no_of_suv_plus_rides': 'sum',
    'total_dist_travelled_in_suv_plus': 'sum',
    'total_fair_paid_in_suv_plus': 'sum',
    'total_no_of_taxi_rides': 'sum',
    'total_dist_travelled_in_taxi': 'sum',
    'total_fair_paid_in_taxi': 'sum',
    'total_no_of_taxi_plus_rides': 'sum',
    'total_dist_travelled_in_taxi_plus': 'sum',
    'total_fair_paid_in_taxi_plus': 'sum'
}

# List of columns that should sum up the values for df1 and _final_df
sum_columns = [
    'total_searches', 'total_bookings', 'ByDriverCanc', 'ByUserCanc', 'ByAppOrMerchantCanc',
    'total_rides_overall', 'total_dist_travelled_overall', 'total_fair_paid', 
    'total_no_of_auto_rides', 'total_dist_travelled_in_auto', 'total_fair_paid_in_auto',
    'total_no_of_bike_rides', 'total_dist_travelled_in_bike', 'total_fair_paid_in_bike',
    'total_no_of_delivery_bike_rides', 'total_dist_travelled_in_delivery_bike', 'total_fair_paid_in_delivery_bike',
    'total_no_of_hatchback_rides', 'total_dist_travelled_in_hatchback', 'total_fair_paid_in_hatchback',
    'total_no_of_sedan_rides', 'total_dist_travelled_in_sedan', 'total_fair_paid_in_sedan',
    'total_no_of_suv_rides', 'total_dist_travelled_in_suv', 'total_fair_paid_in_suv',
    'total_no_of_suv_plus_rides', 'total_dist_travelled_in_suv_plus', 'total_fair_paid_in_suv_plus',
    'total_no_of_taxi_rides', 'total_dist_travelled_in_taxi', 'total_fair_paid_in_taxi',
    'total_no_of_taxi_plus_rides', 'total_dist_travelled_in_taxi_plus', 'total_fair_paid_in_taxi_plus'
]

# Add sum aggregation for the sum_columns
for col in sum_columns:
    aggregations[col] = 'sum'

# Now, apply aggregation
merged_df = merged_df.groupby('rider_id').agg(aggregations).reset_index()

# Now apply the custom logic for rides_in_first_30_days
# First, we need to apply the condition: if 'days_since_reg' <= 30, take the max; otherwise, keep the value from df1

def custom_rides_in_first_30_days(row):
    # Check if 'days_since_reg' from _final_df is less than or equal to 30
    if row['days_since_reg'] <= 30:
        return max(row['rides_in_first_30_days_df1'], row['rides_in_first_30_days_final_df'])
    else:
        return row['rides_in_first_30_days_df1']

# Apply the custom function
merged_df['rides_in_first_30_days'] = merged_df.apply(custom_rides_in_first_30_days, axis=1)

# Drop the individual columns used for the calculation (optional)
merged_df.drop(columns=[
    'rides_in_first_30_days_df1', 'rides_in_first_30_days_final_df', 'days_since_reg'
], inplace=True)

merged_df['gap_in_first_n_last_booking'] = (pd.to_datetime(merged_df['last_booking_date']) - pd.to_datetime(merged_df['first_booking_date'])).dt.days

#  save to a CSV
# merged_df.to_csv('path_to_result.csv', index=False)




## Inserting data into table
insert_data_from_csv(merged_df,'atlas_app.person_meta')