from datetime import datetime, timedelta
import time
import logging
from azure.storage.blob import ContainerClient
import azure.functions as func
import pandas as pd
from pandas.core.frame import DataFrame
from sklearn.ensemble import RandomForestClassifier
import pickle
from dateutil.parser import parse
import math
import numpy as np
import os

road_stations = {"SN79791": "E6 Saltfjellet", "SN84905": "E10 Bjørnfjell", "SN94195": "E6 Sennalandet" }

def datetime_from_utc_to_local(utc_datetime):
    now_timestamp = time.time()
    offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset

def build_file_name(station_id, obs, day, interval):
    dict_obs = {1:'00',2:'06',3:'12',4:'18' }
    dict_interval = {1:'00_06',2:'06_12',3:'12_18',4:'18_24' }
    return "{0}_obs{1}_d{2}{3}.pickle".format(station_id, dict_obs.get(obs), day, dict_interval.get(interval))

def main(mytimer: func.TimerRequest, outputBlob: func.Out[str]) -> None: 
    logger = logging.getLogger("logger_name")
    logger.disabled = True
 
    blob_service_actuals = ContainerClient.from_connection_string(os.environ['Blockblob'], "actuals", logger=logger)
    blob_service_pickles = ContainerClient.from_connection_string(os.environ['Blockblob'], "treepickles", logger=logger)

    f = blob_service_actuals.download_blob("weather_predictions.json")
    s = blob_service_actuals.download_blob("snow_depths.json")
    o = blob_service_actuals.download_blob("weather_observations.json")

    df = pd.read_json(f.content_as_text())
    dfSnow = pd.read_json(s.content_as_text())
    dfObs = pd.read_json(o.content_as_text())

    dfOut = DataFrame(columns=['station_id', 'station_name', 'weather_forecast_ref_time', 'obs', 'day', 'interval', 'prediction_sort_counter', 'prediction_time_from', 'prediction_time_to', 'closed_prediction'])
    for station_id in df["station_id"].unique(): 

        snow_depth = dfSnow[dfSnow['Station_id']==station_id].reset_index().get('Snødybde').fillna(0)
        air_temp = dfObs[dfObs['Station_id']==station_id].reset_index().get('air_temp').fillna(0)
        relative_humidity = dfObs[dfObs['Station_id']==station_id].reset_index().get('relative_humidity').fillna(0)
        dew_point_temp = dfObs[dfObs['Station_id']==station_id].reset_index().get('dew_point_temp').fillna(0)
        wind_speed = dfObs[dfObs['Station_id']==station_id].reset_index().get('wind_speed').fillna(0)
        wind_bearing= dfObs[dfObs['Station_id']==station_id].reset_index().get('wind_bearing').fillna(0)
        min_visibility_dist= dfObs[dfObs['Station_id']==station_id].reset_index().get('min_visibility_dist').fillna(0)
        precipitation_intensity= dfObs[dfObs['Station_id']==station_id].reset_index().get('precipitation_intensity').fillna(0)
        road_temp = dfObs[dfObs['Station_id']==station_id].reset_index().get('road_temp').fillna(0)

        for obs in range(1, 5, 1):  # 4 observasjoner pr døgn
            for day in df["prediction_day"].unique():  # looper gjennom predikerte dager [0,1,2]
                if day < 3:  # tar bort værmeldinger for 3 dager frem.
                    for interval in df["prediction_interval"].unique():  # looper gjennom de fire tidsintervallene pr dag
                        dfInput = DataFrame()
                        for h in range(6, 55, 6):
                            r = df[(df['station_id'] == station_id) & (df['prediction_hour'] == h)].copy(deep=True).reset_index()
                            dfInput['h' + str(h) +'_air_temp'] = r['air_temp']
                            dfInput['h' + str(h) +'_precipitation_amount'] = r['precipitation_amount'].fillna(0)
                            dfInput['h' + str(h) +'_wind_bearing_sin'] = np.sin(2 * np.pi *  r['wind_bearing']/360.0)
                            dfInput['h' + str(h) +'_wind_bearing_cos'] = np.cos(2 * np.pi *  r['wind_bearing']/360.0)
                            dfInput['h' + str(h) +'_wind_speed'] = r['wind_speed']
                            dfInput['h' + str(h) +'_cloud_area_fraction'] = r['cloud_area_fraction']/100
                            dfInput['h' + str(h) +'_air_pressure_at_sea_level'] = r['air_pressure_at_sea_level']*100
                            dfInput['h' + str(h) +'_relative_humidity'] = r['relative_humidity']/100

                        dfInput['air_temp'] = air_temp
                        dfInput['relative_humidity'] = relative_humidity
                        dfInput['dew_point_temp'] = dew_point_temp
                        dfInput['wind_speed'] = wind_speed
                        dfInput['min_visibility_dist'] = min_visibility_dist
                        dfInput['precipitation_intensity'] = precipitation_intensity
                        dfInput['road_temp'] = road_temp
                        dfInput['snow_depth'] = snow_depth
                        dfInput['wind_bearing_sin'] = np.sin(2 * np.pi * wind_bearing/360.0)
                        dfInput['wind_bearing_cos'] = np.cos(2 * np.pi * wind_bearing/360.0)
                        dfInput['day_this_winter'] = datetime.today().timetuple().tm_yday - 274 if 0 < datetime.today().timetuple().tm_yday - 274 < 365 else datetime.today().timetuple().tm_yday + 91

                        pic = blob_service_pickles.download_blob(build_file_name(station_id, obs, day, interval)).readall()
                        model = RandomForestClassifier()
                        model = pickle.loads(pic)
                        ynew = model.predict_proba(dfInput.values)
                        obs_now = math.floor(datetime.now().hour / 6) + 1
                        prediction_time_from = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(hours=int((day*24) + ((interval-1)*6)))
                        prediction_time_to = prediction_time_from + timedelta(hours=6)
                        if (obs == obs_now) and (prediction_time_from > datetime.now()):
                            dfOut = dfOut.append({
                                'station_id' : station_id,
                                'station_name' : road_stations.get(station_id),
                                'weather_forecast_ref_time': datetime_from_utc_to_local(parse(r['forecast_ref_time_zulu'][0])).strftime("%d.%m.%Y %H:%M:%S"),
                                'obs': obs,
                                'day': day,
                                'interval': interval,
                                'prediction_sort_counter': prediction_time_from,
                                'prediction_time_from': prediction_time_from.strftime("%d.%m.%Y %H:%M:%S"),
                                'prediction_time_to': prediction_time_to.strftime("%d.%m.%Y %H:%M:%S"),
                                'closed_prediction': ynew[0][1]
                                }, ignore_index=True) 

    dfOut = dfOut.sort_values(by=['station_id', 'obs', 'day', 'interval'])
    outputBlob.set(dfOut.to_json(orient='records',force_ascii=False, indent=2, index=True))
                            

                    
