import datetime
import logging
import requests
import pandas as pd
import numpy as np
import azure.functions as func
from azure.storage.blob import BlobServiceClient
from bs4 import BeautifulSoup
from dateutil.parser import parse
from pytz import timezone

def main(mytimer: func.TimerRequest, outputBlob: func.Out[str]) -> None:
    utc_timestamp = datetime.datetime.utcnow().replace(
        tzinfo=datetime.timezone.utc).isoformat()

    url= 'https://www.vegvesen.no/ws/no/vegvesen/veg/trafikkpublikasjon/vaer/2/GetMeasuredWeatherData'
    user = 'TjeDatexSensConsulting'
    pwd = r'qWrp8n8EMJWvZDw4dAu5'

    blob_service_client = BlobServiceClient.from_connection_string("AccountName=man5101;AccountKey=Ua1xq+7LngswCSlZ5AhAX37Ri64Rn9h6rLIqIWdDeh0OYCPpFbePbXy8K9D5IsSL/CYD61QQNbepusMgJHyLOw==")
    f = blob_service_client.get_blob_client("actuals", 'weather_observations.json')
    # json_content = f.download_blob().readall()
    # dfExisting = pd.read_json(json_content)

    road_ids = {
        "SN79791": 80, 
        "SN84905": 323, 
        "SN94195": 228
    }

    df_out = pd.DataFrame(columns = \
        ['Station_id'
        ,'observation_time'
        , 'air_temp'
        , 'relative_humidity'
        , 'dew_point_temp'
        , 'wind_speed'
        , 'wind_bearing'
        , 'min_visibility_dist'
        , 'precipitation_intensity'
        , 'road_friction'
        , 'road_temp'
         ] )

    response = requests.get(url, auth=(user, pwd))
    soup = BeautifulSoup(response.content,'xml')

    for station_id, road_id in road_ids.items():
        site = soup.find('measurementSiteReference', id=road_id).parent
        #logging.info(site)
        df_out = df_out.append({
            "Station_id": station_id
            , 'observation_time' : np.nan if site.find('measurementTimeDefault') is None else parse(site.find('measurementTimeDefault').get_text()).astimezone(timezone('Etc/UTC'))
            , 'air_temp' : np.nan if site.find('airTemperature') is None else site.find('airTemperature').string
            , 'relative_humidity' : np.nan if site.find('relativeHumidity') is None else site.find('relativeHumidity').string
            , 'dew_point_temp' : np.nan if site.find('dewPointTemperature') is None else site.find('dewPointTemperature').string
            , 'wind_speed' : np.nan if site.find('windSpeed') is None else site.find('windSpeed').string
            , 'wind_bearing' : np.nan if site.find('windDirectionBearing') is None else site.find('windDirectionBearing').string
            , 'min_visibility_dist' : np.nan if site.find('minimumVisibilityDistance') is None else site.find('minimumVisibilityDistance').string 
            , 'precipitation_intensity' : np.nan if site.find('precipitationIntensity') is None else site.find('precipitationIntensity').string
            , 'road_friction' : np.nan if site.find('friction') is None else site.find('friction').string
            , 'road_temp' : np.nan if site.find('roadSurfaceTemperature') is None else site.find('roadSurfaceTemperature').string
        }, ignore_index=True)
        
        #logging.info(station_id + ' at %s', utc_timestamp)
         
    outputBlob.set(df_out.to_json(orient='records',force_ascii=False))
 