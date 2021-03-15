from datetime import datetime, timedelta
import time
import requests
import azure.functions as func
import pandas as pd
from dateutil.parser import parse
import numpy as np

def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset

def prediction_time_interval(dt):
    if dt.hour < 6: return "1"
    if 6 <= dt.hour < 12: return "2"
    if 12 <= dt.hour < 18: return "3"
    if dt.hour >= 18: return "4"

def prediction_day(dt):
    if dt.date() == datetime.today().date(): return "0"
    if dt.date() ==  datetime.today().date() + timedelta(days=1): return "1"
    if dt.date() ==  datetime.today().date()  + timedelta(days=2): return "2"
    if dt.date() ==  datetime.today().date()  + timedelta(days=3): return "3"

def main(mytimer: func.TimerRequest, outputBlob: func.Out[str]) -> None:

    url_list = {
        "SN79791": "https://api.met.no/weatherapi/locationforecast/2.0/complete?lat=66.555&lon=15.3234", 
        "SN84905": "https://api.met.no/weatherapi/locationforecast/2.0/complete?lat=68.4363&lon=18.1029", 
        "SN94195": "https://api.met.no/weatherapi/locationforecast/2.0/complete?lat=70.2788&lon=24.1006"
        }
    headers = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64; rv:78.0) Gecko/20100101 Firefox/78.0'}
    df = pd.DataFrame()

    for station_id, baseurl in url_list.items():
        for hour in range(6,56,6):
            r = requests.get(baseurl, headers=headers)
            weather_info = r.json()
            df = df.append({'station_id' : station_id,
                'forecast_time' : datetime_from_utc_to_local(parse(weather_info['properties']['timeseries'][hour]['time'])).strftime("%d.%m.%Y %H:%M:%S"),
                'forecast_ref_time' : datetime_from_utc_to_local(parse(weather_info['properties']['meta']['updated_at'])).strftime("%d.%m.%Y %H:%M:%S"),
                'forecast_time_zulu' : weather_info['properties']['timeseries'][hour]['time'],
                'forecast_ref_time_zulu' : weather_info['properties']['meta']['updated_at'],
                'prediction_interval' : int(prediction_time_interval(datetime_from_utc_to_local(parse(weather_info['properties']['timeseries'][hour]['time'])))),
                'prediction_day' : int(prediction_day(parse(weather_info['properties']['timeseries'][hour]['time']))),
                'prediction_hour' : int(hour),
                'air_pressure_at_sea_level' : weather_info['properties']['timeseries'][hour]['data']['instant']['details']['air_pressure_at_sea_level'],
                'air_temp' : weather_info['properties']['timeseries'][hour]['data']['instant']['details']['air_temperature'],
                'cloud_area_fraction' : weather_info['properties']['timeseries'][hour]['data']['instant']['details']['cloud_area_fraction'],
                'dew_point_temp' : weather_info['properties']['timeseries'][hour]['data']['instant']['details']['dew_point_temperature'],
                'precipitation_amount' : np.nan if hour >= 48 else weather_info['properties']['timeseries'][hour]['data']['next_6_hours']['details']['precipitation_amount'],
                'precipitation_probability' : np.nan if hour >= 48 else weather_info['properties']['timeseries'][hour]['data']['next_6_hours']['details']['probability_of_precipitation'],
                'relative_humidity' : weather_info['properties']['timeseries'][hour]['data']['instant']['details']['relative_humidity'],
                'wind_bearing' : weather_info['properties']['timeseries'][hour]['data']['instant']['details']['wind_from_direction'],
                'wind_speed' : weather_info['properties']['timeseries'][hour]['data']['instant']['details']['wind_speed'],
                'wind_speed_of_gust' : np.nan if hour >= 48 else weather_info['properties']['timeseries'][hour]['data']['instant']['details']['wind_speed_of_gust']
                }, ignore_index=True)
    
    outputBlob.set(df.to_json(orient='records',force_ascii=False, indent=2))
