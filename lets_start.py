# region 0. import data & enter input_date
import pandas as pd
import os
import numpy as np
# Import gtfs files
# Path to the directory containing GTFS files
gtfs_path = r'C:\Users\kieuk\Downloads\50c PT trial report\50c PT trial report_no git atm\gtfs_12_03_2024'
calendar_df = pd.read_csv(os.path.join(gtfs_path, 'calendar.txt'))
routes_df = pd.read_csv(os.path.join(gtfs_path, 'routes.txt'))
stops_times_df = pd.read_csv(os.path.join(gtfs_path, 'stop_times.txt'))
stops_df = pd.read_csv(os.path.join(gtfs_path, 'stops.txt'))
trips_df = pd.read_csv(os.path.join(gtfs_path, 'trips.txt'))
distance_to_Roma = pd.read_csv(r'C:\Users\kieuk\Downloads\50c PT trial report\50c PT trial report_no git atm\list_of_rail_station_&_distance_to_Roma.csv')
# Import tickets_data
ticket_path = r'C:\Users\kieuk\Downloads\50c PT trial report\50c PT trial report_no git atm'
tickets_data_df = pd.read_csv(os.path.join(ticket_path,'snowflake_ticket_12_03_2024.csv'))

# Enter input_date format yyyymmdd
input_date = 20240312
# endregion

# region 1. "Trip_direction" & "Service_line" for gtfs "trip.txt" and "stop_times.txt": trip_table_df and stop_times_df will be the final objects/results of region 1 to match with cleaned results of ticket_data in region 2
# create trips_table_df with additional 'trip_direction', 'service_line' columns
trips_df['Trip_direction'] = ''
trips_df['Service_line'] =''
trips_table_df = trips_df.copy()
stops_times_df['Trip_direction']=''
stops_times_df['Service_line']=''
stops_times_df['stop_name']=''
stops_times_df['distance to Roma Street Station']=''
stops_times_table_df = stops_times_df.copy()
# endregion

# region 1.1 Select correct QR 'service_id' in 'calendar.txt' as 'qr_service_id':
# Filter rows where 'service_id' contains 'QR'
qr_calendar = calendar_df[calendar_df['service_id'].str.contains('QR')]
# Search for service_id for input_date 

# Exact match where input_date is equal to start_date or end_date
exact_match = qr_calendar[
    (qr_calendar['start_date'] == input_date) |
    (qr_calendar['end_date'] == input_date)
]

if not exact_match.empty:
    # Extract and print the service_id(s) with exact match
    qr_service_id = exact_match['service_id'].tolist()
    print("Exact Service ID(s) found:")
    print(qr_service_id)
else:
    # Find likely matches where input_date is within the start_date and end_date range
    likely_match = qr_calendar[
        (qr_calendar['start_date'] < input_date) &
        (qr_calendar['end_date'] > input_date)
    ].copy()

    if not likely_match.empty:
        # Calculate the duration between start_date and end_date
        likely_match['duration'] = likely_match['end_date'] - likely_match['start_date']
        
        # Find the row with the minimum time duration
        min_duration_row = likely_match.loc[likely_match['duration'].idxmin()]
        
        # Extract and print the service_id with the least duration
        qr_service_id = min_duration_row['service_id']
        to_print_1 = " 1. Chosen service_id for input_date: "+ str(input_date)+ " is "+ qr_service_id 
        print(to_print_1)
        print("       Please check gtfs 'calendar' again just in case.")
    else:
        print("ERROR: can't find service_id. Have we imported the right gtfs_calendar files?")
# endregion

# region 1.2 Filter the trips_table_df with the found qr_serive_id: this is all train trips made in the input date
trips_table_df = trips_table_df.query('service_id == @qr_service_id')
to_print_2= ' 2. These are filtered QR trips made in input_date of '+ str(input_date)+ ' under service_id:'+ qr_service_id
print (to_print_2)
#print(trips_table_df)
# endregion

# region 1.3 Filter stop_times based on trip_ids found in 1.2: 'stops_times_filtered' include stop_sequence to decide direction of trip and potential service line

trip_ids_to_match = trips_table_df['trip_id'].unique() # Get the list of trip_ids from trips_table_df
stops_times_filtered = stops_times_table_df[stops_times_table_df['trip_id'].isin(trip_ids_to_match)] # Filter stop_times_table_df to include only matching trip_ids
# endregion

# region 1.4 Apply some logic to 'route_id' in trips_table_df to determine Direction & Service_line in 

#   region 1.4.1 Adding 'stop_name' and 'Distance to Roma' to 'stops_times_filtered'
#     region 1.4.1.1 Adding stop_name 
stops_df = stops_df[pd.to_numeric(stops_df['stop_id'], errors='coerce').notnull()] # Remove non-numeric values in 'stop-id' in "stops_df"
# Ensure stop_id in both DataFrames are of the same type
stops_df['stop_id'] = stops_df['stop_id'].astype(int)
stops_times_filtered['stop_id'] = stops_times_filtered['stop_id'].astype(int)

# Merge DataFrames on 'stop_id' to get stop_name
stops_times_filtered = stops_times_filtered.merge(
    stops_df[['stop_id', 'stop_name']], 
    on='stop_id',
    how='left'  # Left join to keep all rows from stops_times_filtered_df
)
# Drop the old 'stop_name' column if it exists and rename the new 'stop_name' column
if 'stop_name_x' in stops_times_filtered.columns:
    stops_times_filtered.drop(columns='stop_name_x', inplace=True)

if 'stop_name_y' in stops_times_filtered.columns:
    stops_times_filtered.rename(columns={'stop_name_y': 'stop_name'}, inplace=True)
stops_times_filtered['stop_name'] = stops_times_filtered['stop_name'].str.split(' station', n=8).str[0]
# endregion
#     region 1.4.1.2 Adding Distance_to_Roma
#        Merge DataFrames on the common column (stop_name / 'Station' in "distance to Roma")
stops_times_filtered = stops_times_filtered.merge(
    distance_to_Roma,
    left_on='stop_name',  # in stops_times_filtered
    right_on='Station',  # in distance_to_Roma
    how='left'  # keep all rows from stops_times_filtered
)
#        Drop the redundant 'station_name' column
stops_times_filtered.drop(columns='Station', inplace=True)
stops_times_filtered.drop(columns='distance to Roma Street Station', inplace=True)  
stops_times_filtered.rename(columns={'distance': 'Distance to Roma Street Station'}, inplace=True)      # Rename the 'distance' column
# endregion
# endregion
#   region 1.4.2 Functions to update Trip_Direction and Service_Line
#                Update based on route_id with 'BR' in route_codes
trips_table_df.loc[trips_table_df['route_id'].str[:2] == 'BR', 'Trip_direction'] = 'Outbound'
trips_table_df.loc[trips_table_df['route_id'].str[:2] == 'BR', 'Service_line'] = trips_table_df['route_id'].str[2:4]

trips_table_df.loc[trips_table_df['route_id'].str[2:4] == 'BR', 'Trip_direction'] = 'Inbound'
trips_table_df.loc[trips_table_df['route_id'].str[2:4] == 'BR', 'Service_line'] = trips_table_df['route_id'].str[:2]
print(trips_table_df['Service_line'].tolist())
#                Update non'BR' in route_codes
def update_trip_directions_and_lines(trips_table_df, stops_times_filtered, distance_to_Roma):
    # Find non-BR trip_ids
    no_br_trip_ids = trips_table_df[~trips_table_df['route_id'].str[:4].str.contains('BR')]['trip_id']
  
    # Apply direction_finder_for_non_br_trips to the filtered trip_ids
    direction_finder_for_non_br_trips(no_br_trip_ids)

    def direction_finder_for_non_br_trips(trip_ids):
        # Helper function to check if "Roma Street" is in the stop_times_filtered for specific trip_ids
        def use_distance_to_Roma(trip_ids):
            for trip_id in trip_ids:
                trip_df = stops_times_filtered[stops_times_filtered['trip_id'] == trip_id]

                if not trip_df.empty:
                    min_stop_seq = trip_df['stop_sequence'].min()
                    max_stop_seq = trip_df['stop_sequence'].max()

                    min_distance = trip_df[trip_df['stop_sequence'] == min_stop_seq]['Distance to Roma Street Station'].min()
                    max_distance = trip_df[trip_df['stop_sequence'] == max_stop_seq]['Distance to Roma Street Station'].max()

                    # Update direction based on distance comparison
                    if min_distance > max_distance:
                        trips_table_df.loc[trips_table_df['trip_id'] == trip_id, 'Trip_direction'] = 'Inbound'
                        trips_table_df.loc[trips_table_df['trip_id'] == trip_id, 'Service_line'] = trips_table_df['route_id'].str[:2]
                    else:
                        trips_table_df.loc[trips_table_df['trip_id'] == trip_id, 'Trip_direction'] = 'Outbound'
                        trips_table_df.loc[trips_table_df['trip_id'] == trip_id, 'Service_line'] = trips_table_df['route_id'].str[:2]

        # Check for "Roma Street" and apply distance-based updates
        roma_st_trip_ids = stops_times_filtered[stops_times_filtered['stop_name'].str.contains("Roma Street", case=False, na=False)]['trip_id']
        inbound_trips = set(trip_ids) & set(roma_st_trip_ids)
        
        if inbound_trips:
            trips_table_df.loc[trips_table_df['trip_id'].isin(inbound_trips), 'Trip_direction'] = 'Inbound'
            trips_table_df.loc[trips_table_df['trip_id'].isin(inbound_trips), 'Service_line'] = trips_table_df['route_id'].str[:2]
        else:
            use_distance_to_Roma(trip_ids)
    
    return trips_table_df
               
print(trips_table_df)
# Apply the function to update the DataFrame
trips_table_df_updated = update_trip_directions_and_lines(trips_table_df, stop_times_filtered_df)

# Print the updated DataFrame
print(trips_table_df_updated)
# endregion
# endregion   
# region 2. "train_trip_id_finder" for passenger trips in tickets dataset:
# region 2.1 Matching "boarding station', 'alighting stations' to 'stop_id' and associated "arrival_time", "departure_time" in "stop_times":
if not existed, flag this trip as 'journey',
# endregion
2.2 split this 'journey' as 2 trips with potential matching transfer station
 do 2.1 again for both splitted trip until found 'trip_id'

This step need to find correct last trips in each journey based on the alighting time (2 mins window) matching with train arrival time at alighting station then find potential previous trip based on sensible travel time between transfer station to destination of last trip 
#endregion
# region 3. "train_load_calculator"
#endregion
# region 4. More calculator ?
     