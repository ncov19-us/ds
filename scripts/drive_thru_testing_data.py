#!/home/alex/anaconda3/envs/geoprocessing/bin/python3

import requests
from bs4 import BeautifulSoup
import pandas as pd
from geopy.geocoders import ArcGIS
from geopy.extra.rate_limiter import RateLimiter

def markers_to_dataframe():
    """This function takes the https://covid19.sokat.ai/ web map as input to generate a CSV file of drive-thru testing locations. 
    """
    # URL of web map with data
    url = "https://covid19.sokat.ai/"
    
    # Download webpage data with requests
    page = requests.get(url)
    
    # Rarse the webpage data with BeautifulSoup
    soup = BeautifulSoup(page.content, 'html.parser')
    
    # Select scripts in HTML file
    scripts = list(soup.find_all('script'))
    
    # Subset for the script with marker data and begin string splitting
    html = str(list(scripts[7])).split('var marker')
    
    # Use for-loops to generate the names, coordinates, and URLs of drive-thru testing locations
    list_of_names = []
    list_of_coords = []
    list_of_urls = []
    
    for i in range(2,len(html)-1):
        list_of_names.append(html[i].split('target="_blank">')[1].split('</a></div>')[0])
        list_of_coords.append(html[i].split('target="_blank">')[0].split("L.marker(\\n                [")[1].split("]")[0])
        list_of_urls.append(html[i].split('target="_blank">')[0].split('><a href="')[1].split('"')[0])

    # Concatenate lists into a pandas DataFrame
    dataframe = pd.DataFrame([list_of_names, list_of_coords, list_of_urls]).T
    
    # Rename the columns with appropriate titles
    dataframe.columns = ["Name","Coordinate", "URL"]
    
    # Convert coordinates column to a string type to pass to reverse geocoder
    dataframe["Coordinate"] = dataframe["Coordinate"].astype(str)
    
    # Instantiate a geocoder and impose a rate limiter
    geolocator = ArcGIS(user_agent="drive-thru-testing-reverse-geocoding")
    geocode = RateLimiter(geolocator.geocode, min_delay_seconds=1)
    
    # Define a convenience function to clean and reverse geocode coordinates cell contents, returns address
    def reverse_geocode(cell_contents):
        cellcontents = cell_contents.replace("(","").replace(")","")
        location = geolocator.reverse(cellcontents)
        return location.address
    
    # Apply function to dataframe
    dataframe["Addresses"] = dataframe["Coordinate"].apply(reverse_geocode)
    
    # Remove repetitive USA suffix
    dataframe["Addresses"] = dataframe["Addresses"].str.replace(", USA", "")
    
    # Create columns for state, city, and street address with string splitting and regular expressions
    dataframe["State"] = dataframe["Addresses"].str.split(",").str[-1].str.replace('\d+', '')
    dataframe["City"] = dataframe["Addresses"].str.split(",").str[-2]
    dataframe["Street Address"] = dataframe["Addresses"].str.split(",").str[-3]
    
    # Drop the addresses column with unpleasantly long string data
    dataframe.drop(["Addresses"], axis=1, inplace=True)
    
    # Save results as a CSV file
    dataframe.to_csv("../drive_thru_testing_locations/drive-thru-testing-data.csv", index=0)

if __name__ == "__main__":
    markers_to_dataframe()