import re
import requests 
from pprint import pprint
from bs4 import BeautifulSoup as bs 
from datetime import datetime
import os


def get_data_urls():
    
    r = requests.get('https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page') 
    soup = bs(r.content)
    web_links = soup.select('a') 
    actual_web_links = [web_link['href'] for web_link in web_links]


    p = re.compile('.*tripdata.*.parquet')
    
    filtered_links = [ s for s in actual_web_links if p.match(s) ]

    re2 = re.compile(".*yellow.*")
    
    yellow_taxi_links = [ s for s in filtered_links if re2.match(s) ]

    current_year = datetime.now().year

    start_year = "2019"

    filtered_yellow_taxi_links = {}
    for i in range(int(start_year), current_year):
        filtered_yellow_taxi_links.update(
            {
                f"{i}_links": [link for link in yellow_taxi_links if str(i) in link]
            }
        )

    return filtered_yellow_taxi_links


def save_data_from_links(link, location):
    file_name = link.split("/")[-1]
    r = requests.get(link)
    with open(os.path.join(location,file_name), "wb") as f:
        f.write(r.content)


def get_taxi_trip_data():

    urls = get_data_urls()

    for item in urls.keys():

        path = os.path.join("data", item.split("_")[0])
        if not os.path.exists(path):
            os.makedirs(path)
        
        for url in urls[item]:
            save_data_from_links(url, path)