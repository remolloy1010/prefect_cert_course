import httpx


# def fetch_weather(lat: float, lon: float):
#     base_url = "https://api.open-meteo.com/v1/forecast/"
#     weather = httpx.get(
#         base_url,
#         params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
#     )
#     most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
#     print(f"Most recent temp C: {most_recent_temp} degrees")
#     return most_recent_temp


# if __name__ == "__main__":
#     fetch_weather(38.9, -77.0)

import httpx
from prefect import flow, task


# @flow()
# def fetch_weather(lat: float, lon: float):
#     base_url = "https://api.open-meteo.com/v1/forecast/"
#     weather = httpx.get(
#         base_url,
#         params=dict(latitude=lat, longitude=lon, hourly="temperature_2m"),
#     )
#     most_recent_temp = float(weather.json()["hourly"]["temperature_2m"][0])
#     print(f"Most recent temp C: {most_recent_temp} degrees")
#     return most_recent_temp



@task
def fetch_metrics(lat: float, lon: float, metric: str):
    base_url = "https://api.open-meteo.com/v1/forecast/"
    weather = httpx.get(
        base_url,
        params=dict(latitude=lat, longitude=lon, hourly=metric),
    )
    return weather

@task
def get_hourly_data(weather, metric: str):
    most_recent_metric = float(weather.json()["hourly"][metric][0])
    return most_recent_metric

@task
def save_weather(metric_val: float, metric: str):
    with open(''.join([metric, ".csv"]), "w+") as w:
        w.write(str(metric_val))
    return "Successfully wrote csv"

@flow
def get_all_metrics(lat: float, lon: float, metric_data: dict):
    for metric in metric_data.keys():
        weather = fetch_metrics(lat, lon, metric)
        most_recent_temp = get_hourly_data(weather, metric)
        print(f"Most recent temp C: {most_recent_temp} degrees")

if __name__ == "__main__":
    get_all_metrics(38.9, -77.0, {
        "temperature" : ["temperature_2m", "degrees"],
        "windspeed" : ["windspeed_10m", "km/h"],
        "precipitation" : ["precipitation", "mm"]})