## Pipeline
import pandas as pd

# Creating file path
airports_path = 'C:\\Users\\Kimo Store\\Downloads\\airports.csv'
airlines_path = 'C:\\Users\\Kimo Store\\Downloads\\airlines.csv'
routes_path = 'C:\\Users\\Kimo Store\\Downloads\\routes.csv'


from geopy.distance import geodesic
from sqlalchemy import create_engine
import pymysql

# Database connection setup
db_user = 'root'
db_password = 'Nour@2002'
db_host = 'localhost'
db_name = 'flight_data'

# Use quote() to properly escape the password in the connection string
from urllib.parse import quote_plus
# Quote the password using quote_plus to handle special characters
quoted_password = quote_plus(db_password)
engine = create_engine(f"mysql+pymysql://{db_user}:{quoted_password}@{db_host}:3306/{db_name}")


# Step 1: Extract - Load the data
airlines_df = pd.read_csv(airlines_path)
airports_df = pd.read_csv(airports_path)
routes_df = pd.read_csv(routes_path)

# Adding names to columns
airlines_df.columns = ['Airline ID', 'Name', 'Alias', 'IATA', 'ICAO', 'Callsign', 'Country', 'Active']
airports_df.columns = ['Airport ID', 'Name', 'City', 'Country', 'IATA', 'ICAO', 'Latitude', 'Longitude', 'Altitude', 'Timezone', 'DST', 'Tz database time zone', 'Type', 'Source']
routes_df.columns = ['Airline', 'Airline ID', 'Source airport', 'Source airport ID', 'Destination airport', 'Destination airport ID', 'Codeshare', 'Stops', 'Equipment']

# Step 2: Transform - Data Cleaning and Enrichment
# Replace "\N" with None
airlines_df.replace(r'\N', None, inplace=True)
airports_df.replace(r'\N', None, inplace=True)
routes_df.replace(r'\N', None, inplace=True)

# Drop rows where essential fields are missing
airlines_df.dropna(subset=["Name", "Country"], inplace=True)
airports_df.dropna(subset=["Latitude", "Longitude"], inplace=True)
routes_df.dropna(subset=["Airline", "Source airport", "Destination airport"], inplace=True)

# Set default for Codeshare
routes_df['Codeshare'].fillna('N', inplace=True)

# Merge airport coordinates into routes for geospatial calculations
routes_with_coords = routes_df.merge(
    airports_df[['IATA', 'Latitude', 'Longitude']].rename(columns={'IATA': 'Source airport', 'Latitude': 'Source_lat', 'Longitude': 'Source_lon'}),
    on='Source airport', how='left'
).merge(
    airports_df[['IATA', 'Latitude', 'Longitude']].rename(columns={'IATA': 'Destination airport', 'Latitude': 'Dest_lat', 'Longitude': 'Dest_lon'}),
    on='Destination airport', how='left'
)

# Drop rows where either source or destination coordinates are missing
routes_with_coords.dropna(subset=['Source_lat', 'Source_lon', 'Dest_lat', 'Dest_lon'], inplace=True)

# Calculate distances in kilometers
routes_with_coords['Distance_km'] = routes_with_coords.apply(
    lambda row: geodesic((row['Source_lat'], row['Source_lon']), (row['Dest_lat'], row['Dest_lon'])).kilometers, axis=1
)

# Step 3: Load - Insert data into MySQL tables
# Insert airlines data
airlines_df.to_sql('airlines', con=engine, if_exists='replace', index=False)

# Insert airports data
airports_df.to_sql('airports', con=engine, if_exists='replace', index=False)

# Insert routes data with distances
routes_with_coords[['Airline', 'Airline ID', 'Source airport', 'Source airport ID',
                    'Destination airport', 'Destination airport ID', 'Codeshare', 'Stops',
                    'Equipment', 'Distance_km']].to_sql('routes', con=engine, if_exists='replace', index=False)

print("ETL process completed successfully!")


## Insights
# How could airlines reduce environmental impact?
import matplotlib.pyplot as plt
import seaborn as sns

# Count routes by the number of stops
stops_count = routes_df['Stops'].value_counts().sort_index()

# Plot the number of routes by stops
plt.figure(figsize=(8, 6))
sns.barplot(x=stops_count.index, y=stops_count.values, palette="viridis")
plt.xlabel('Number of Stops')
plt.ylabel('Number of Routes')
plt.title('Number of Routes by Stop Count')
plt.show()

# Discuss the opportunities for a new airline venture.
# Count the number of airlines serving each route (source-destination combination)
route_counts = routes_df.groupby(['Source airport', 'Destination airport']).size().reset_index(name='num_airlines')
underserved_routes = route_counts[route_counts['num_airlines'] == 1].sort_values(by='num_airlines').head(10)

# Plot underserved routes
plt.figure(figsize=(10, 6))
sns.barplot(y=underserved_routes['Source airport'] + ' to ' + underserved_routes['Destination airport'], 
            x=underserved_routes['num_airlines'], color='skyblue')
plt.xlabel('Number of Airlines')
plt.ylabel('Routes (Source to Destination)')
plt.title('Top Underserved Routes')
plt.show()

# Select an underperforming airport and discuss how they could improve their positioning.
# Count the number of routes per airport
airport_route_counts = routes_df['Source airport'].value_counts().head(10)

# Plot the top airports by number of routes
plt.figure(figsize=(10, 6))
sns.heatmap(airport_route_counts.to_frame(), annot=True, cmap="YlGnBu", cbar_kws={'label': 'Route Count'})
plt.title('Top Airports by Number of Routes')
plt.xlabel('Airports')
plt.ylabel('Route Frequency')
plt.show()

