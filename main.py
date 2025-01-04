import json
from datetime import datetime
import pandas as pd
import requests

city_name = "Warsaw"
base_url = "https://api.openweathermap.org/data/2.5/weather?q="

with open("credentials.txt","r") as f: # simple File I/O to read the hidden api key from a txt file
    api_key = f.read()

full_url = base_url + city_name + "&appid=" + api_key

def ETL_weather_data(url):
    response = requests.get(full_url) # Data Extraction 
    data = response.json()

    def kelvin_to_celcius(temp_in_kelvin): # A function that converts temperature in kelvin to celcius
        temp_in_celcius = temp_in_kelvin - 273.15
        return temp_in_celcius

    def m_to_km(visibility_m): #A function that converts Kilometers to meters
        visibility_km = visibility_m/1000
        return visibility_km


    # Transfoming the extracted data which will be  in json format
    country = data["sys"]["country"]
    city = data["name"]
    weather_description = data["weather"][0]["description"]
    temperature_Celcius = kelvin_to_celcius(data["main"]["temp"])
    feels_like_Celcius = kelvin_to_celcius(data["main"]["feels_like"])
    max_temp_Celcius = kelvin_to_celcius(data["main"]["temp_max"]) 
    min_temp_Celcius = kelvin_to_celcius(data["main"]["temp_min"]) 
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    visibility_km = m_to_km(data["visibility"])

    transformed_data = {"Country":country,
                        "City":city,
                        "Description": weather_description,
                        "Temperature(C)" : temperature_Celcius,
                        "Feals like(C)": feels_like_Celcius,
                        "Maximum Temp(C)": max_temp_Celcius,
                        "Minimum Temp(C)": min_temp_Celcius,
                        "Pressure(hPa)": pressure,
                        "Humidity(%)": humidity,
                        "Wind Speed(m/s)": wind_speed,
                        "Visibility(km)": visibility_km
                        }

    transformed_data_list = [transformed_data]
    df_data = pd.DataFrame(transformed_data_list)

    df_data.to_csv("Current_Weather_Data.csv",index = False) # Loading Transformed data to csv file

ETL_weather_data(full_url)





