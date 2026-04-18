import os
import requests
from dotenv import load_dotenv

load_dotenv()

tomtom_key = os.getenv("TOMTOM_API_KEY")
owm_key = os.getenv("OWM_API_KEY")

print("TOMTOM_API_KEY loaded:", bool(tomtom_key))
print("OWM_API_KEY loaded:", bool(owm_key))

# Test TomTom API
print("\nTesting TomTom API...")
tomtom_url = "https://api.tomtom.com/search/2/search/coffee.json"
tomtom_params = {
    "key": tomtom_key,
    "lat": 40.7128,
    "lon": -74.0060,
    "radius": 1000,
    "limit": 3
}

tomtom_response = requests.get(tomtom_url, params=tomtom_params)
print("TomTom status:", tomtom_response.status_code)
print("TomTom response:", tomtom_response.text[:500])

# Test OpenWeather API
print("\nTesting OpenWeather API...")
owm_url = "https://api.openweathermap.org/data/2.5/weather"
owm_params = {
    "lat": 40.7128,
    "lon": -74.0060,
    "appid": owm_key,
    "units": "metric"
}

owm_response = requests.get(owm_url, params=owm_params)
print("OpenWeather status:", owm_response.status_code)
print("OpenWeather response:", owm_response.text[:500])