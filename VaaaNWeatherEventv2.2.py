
from datetime import datetime
import logging
logging.basicConfig(filename=datetime.now().strftime('%d_%m_%Y.log'),
                    filemode='a',
                    format='%(asctime)s:%(msecs)d-%(name)s-%(levelname)s-%(message)s',
                    datefmt='%d_%m_%Y_%H:%M:%S',level=logging.DEBUG)

try:
    import arrow
    import requests
    import pandas as pd
    import time 
    import json
    import xml.etree.ElementTree as ET
except Exception as e:
    logging.error(e, exc_info=True)


#import pkg_resources.py2_warn

      

try:
    tree = ET.parse('Weather.xml')
    root = tree.getroot()
    config_data = []
    for elem in root:
        for subelem in elem:
            config_data.append(subelem.text)
    conf_user_name = config_data[0]
    conf_password = config_data[1]
    conf_ip = config_data[2]
    conf_service = config_data[3]
    SendUrlweather = config_data[4]
    ReceiveUrl = config_data[5]
    WeatherAPI = config_data[6]
    WeatherPredAPI = config_data[7]
    login_parameter = "{0}/{1}@{2}:1521/{3}".format(conf_user_name,conf_password,conf_ip,conf_service)
except Exception as e:
    logging.error(e, exc_info=True)

t1 = int(time.time())+19800
t2 = t1+86400+19800
# Get first hour of today
start = arrow.now().floor('day')
# Get last hour of today
end = arrow.now().ceil('day')


try:
    try:
        r = requests.get(url = ReceiveUrl) 
        data = r.json()
        df = pd.DataFrame(data)
    except Exception as e:
        logging.error(e, exc_info=True)
    
except Exception as e:
    logging.error(e, exc_info=True)

try:
    print(df.head(10))
    df['new_col'] = list(zip(df.Latitude, df.Longitude))
    latlogList = df["new_col"].unique().tolist()
    LatLongDeviceMap = dict(zip(df.new_col, df.EntryId))

    print(LatLongDeviceMap)
except Exception as e:
        logging.error(e, exc_info=True)
    
def SendEvent():   
    
    t1 = int(time.time())+19800-3600
    t2 = t1+86400+19800
    # Get first hour of today
    start = arrow.now().floor('day')
    # Get last hour of today
    end = arrow.now().ceil('day')

    for k in latlogList:
        latitude = k[0]
        longitude = k[1]
        deviceId = LatLongDeviceMap[k]
        response = requests.get(
          'https://api.stormglass.io/v2/weather/point',
          params={
            'lat': latitude,
            'lng': longitude,
            'params': ','.join(['windSpeed','windDirection','visibility','humidity','pressure','airTemperature','cloudCover']),
            'start': t1,  # Convert to UTC timestamp
            'end': t2 # Convert to UTC timestamp
          },
          headers={
            'Authorization': WeatherAPI
          }
        )

        # Do something with response data.
        json_data = response.json()
        data_list = json_data['hours']
        df = pd.DataFrame(data_list)



        def process(temp):
            try:
                value = temp['noaa']
                return value
            except:
                return temp

        df['airTemperature'] = df['airTemperature'].apply(lambda x: process(x))
        df = df.apply(lambda x: process(x))

        def processtime(temp):
            temp = temp.replace('T',' ')
            temp = temp.split("+")[0]
            return temp

        for col in df.columns:
            if col == 'time':
                df[col] = df[col].apply(lambda x:processtime(x))
            else:
                df[col] = df[col].apply(lambda x:process(x))
        df['DeviceId']=deviceId
        print(df.head(10))
        count = 0
        
        for index, row in df.iterrows():
            #print row['c1'], row['c2']
            DeviceId = row['DeviceId']
            AirTemperature = row['airTemperature']
            CloudCover = row['cloudCover']
            Humidity = row['humidity']
            AirPressure = row['pressure']
            EventDateTime = row['time']
            Visibility = row['visibility']
            WindDirection = row['windDirection']
            WindSpeed = row['windSpeed']
            my_json_string = json.dumps(dict({'DeviceId': DeviceId, 'AirTemperature': AirTemperature,'CloudCover':CloudCover,'Humidity':Humidity,'AirPressure':AirPressure,'EventDateTime':EventDateTime,'Visibility':Visibility,'WindDirection':WindDirection,'WindSpeed':WindSpeed}))
            headers = {'Content-type': 'application/json', 'Accept': 'text/plain'}
            if count == 0:
                count+=1
                r = requests.post(SendUrlweather, my_json_string,headers=headers)
            if count>0 and count<7:
                r = requests.post(WeatherPredAPI, my_json_string,headers=headers)
                count+=1
                #print(r.status_code, r.reason)
                #with open('Weather%d.json'%count, 'w') as json_file:
                    #json.dump(my_json_string, json_file)
                #print(my_json_string)
            else:
                break
    
while True:
    time.sleep(10)
    run_time_hour = datetime.now().strftime('%H')
    run_time_hour = int(run_time_hour)
    print(run_time_hour,type(run_time_hour))
    if run_time_hour%2==0:
        print("Done")
        run_time_min = datetime.now().strftime('%M')
        run_time_min = int(run_time_min)
        time.sleep(10)
        if run_time_min<9:
            print("Done2")
            try:
                print("Done3")
                SendEvent()
            except:
                pass
            time.sleep(900)