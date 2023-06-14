from prefect import flow, task
from prefect.tasks import task_input_hash
import requests as re 
import json 
from statistics import mean 

url = 'https://api.open-meteo.com/v1/forecast'

@task(retries=3, retry_delay_seconds=3)
def fetch_weather_data(url, params):
    response = re.get(url, params)
    return response

@task(retries=3, retry_delay_seconds=3, cache_key_fn=task_input_hash)
def get_elevation(json_object):
    elevation = json.loads(json_object.text)['elevation']
    return elevation 

@flow
def get_daily_temp_30_days(response):
    print(response.json()["daily"]["temperature_2m"])
    # daily = float(response.json()["daily"]["temperature_2m"][0])

@flow
def transform_df(temp_data):
    # hourly_temp_data = temp_data['hourly']
    # df = pd.DataFrame(data = {'Time' : hourly_temp_data['time'], 
    #                      'Temperature' : hourly_temp_data['temperature_2m']})
    data = json.loads(temp_data.text)['hourly']
    avg = mean(data['temperature_2m'])
    return avg

@flow(log_prints=True)
def main():
    # Get some temperature data for a place @ 13.41 Lon 52.52 Lat
    temperature_params = {
        'longitude' : 13.41,
        'latitude' : 52.52,
        'start_date' : '2023-01-01',
        'end_date' : '2023-01-07',
        'hourly' : 'temperature_2m'
        }

    temperature_data = fetch_weather_data(url, temperature_params)
    # https://api.open-meteo.com/v1/forecast?latitude=52.52&longitude=13.41
    print(temperature_data.text)

    
    yemen_elevation = get_elevation(temperature_data)
    avg_temp = transform_df(temperature_data)
    print(f"The elevation off the coast of Yemen is {yemen_elevation}")
    print(f"The average temperature between Jan 1-7th, 2023 was {avg_temp}")

if __name__ == '__main__':
    # do stuff
    print("We are going to fetch some weather data")
    main()
    print("Have a nice day (: ")

  