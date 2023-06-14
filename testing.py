
from prefect import flow, task
from prefect.tasks import task_input_hash
import requests as re 
import json 
from statistics import mean 
url = 'https://api.open-meteo.com/v1/forecast'

# @task(retries=3, retry_delay_seconds=3)
def fetch_weather_data(url, params):
    response = re.get(url, params)
    return response

temperature_params = {
        'longitude' : 13.41,
        'latitude' : 52.52,
        'start_date' : '2023-01-01',
        'end_date' : '2023-01-07',
        'hourly' : 'temperature_2m'
        }

temperature_data = fetch_weather_data(url, temperature_params)

def transform_df(temp_data):
    print(temp_data)
    # hourly_temp_data = temp_data['hourly']
    # df = pd.DataFrame(data = {'Time' : hourly_temp_data['time'], 
    #                      'Temperature' : hourly_temp_data['temperature_2m']})
    data = json.loads(temp_data.text)['hourly']
    avg = mean(data['temperature_2m'])
    avg
    # avg_temp = mean(data['temperature_2m'])
    # print(avg_temp)

print(transform_df(temperature_data))