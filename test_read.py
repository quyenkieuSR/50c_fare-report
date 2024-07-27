import pandas as pd

# Import gtfs files
file_path = r'C:\Users\qzkieu\OneDrive - TMR\Desktop\50c PT trial report\gtfs_12_03_2024\trips.txt'
trips_df = pd.read_csv(file_path)

# Print the first few rows of the DataFrame to verify
print(trips_df.head())
