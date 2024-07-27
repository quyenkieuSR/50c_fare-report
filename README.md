## Purpose:
1. Provide more infomation for current Snowflake dataset regarding the Service Line and Direction bases on the corresponding gtfs feed of that paticular day/time period
2. Clean up outliers trips that is not Rail to Rail.
3. Split supposed rail trips into true trips with realistic train gtfs 'Trip_ID'.
4. Calculate train load using boarding, alighting on each train trip e.g 'Trip_ID'
5. Connecting trips into journeys
6. provide a Count machine
7. validate direction set in "trips" sheet in "202404 2024 AM Peak Crowding schematic" workbook 

## Structure:

1. "Trip_direction" for gtfs "trip.txt":
2. "train_trip_id_finder" for passenger trips in tickets dataset:
    - Matching "boarding station', 'alighting stations' to 'stop_id' and associated "arrival_time", "departure_time" in "stop_times":
if not existed, flag this trip as 'journey',
    -    split this 'journey' as 2 trips with potential matching transfer station
 do 2.1 again for both splitted trip until found 'trip_id'

This step need to find correct last trips in each journey based on the alighting time (2 mins window) matching with train arrival time at alighting station then find potential previous trip based on sensible travel time between transfer station to destination of last trip 

3. "train_load_calculator"

4. 
     