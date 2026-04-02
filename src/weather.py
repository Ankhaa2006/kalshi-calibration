import requests

def get_nyc_forecast():
    """
    NWS API - free, no auth needed
    NYC Central Park coordinates: 40.7789, -73.9692
    """
    # Step 1: get the forecast office and grid for NYC
    points_url = "https://api.weather.gov/points/40.7789,-73.9692"
    headers = {"User-Agent": "kalshi-calibration-research"}
    
    r = requests.get(points_url, headers=headers)
    data = r.json()
    
    forecast_url = data["properties"]["forecast"]
    hourly_url = data["properties"]["forecastHourly"]
    
    print(f"Forecast URL: {forecast_url}\n")
    
    # Step 2: get the daily forecast
    r2 = requests.get(forecast_url, headers=headers)
    periods = r2.json()["properties"]["periods"]
    
    print("=== NWS NYC Forecast ===")
    for p in periods[:6]:
        print(f"{p['name']:<25} {p['temperature']}°{p['temperatureUnit']}  —  {p['shortForecast']}")
    
    # Step 3: get hourly for tomorrow specifically
    print("\n=== Hourly for Tomorrow (Apr 2) ===")
    r3 = requests.get(hourly_url, headers=headers)
    hourly = r3.json()["properties"]["periods"]
    
    apr2_highs = []
    for p in hourly:
        if "2026-04-02" in p["startTime"]:
            temp = p["temperature"]
            apr2_highs.append(temp)
            print(f"  {p['startTime'][11:16]}  {temp}°F")
    
    if apr2_highs:
        print(f"\n  >> NWS predicted HIGH for Apr 2: {max(apr2_highs)}°F")
        print(f"  >> NWS predicted LOW  for Apr 2: {min(apr2_highs)}°F")

get_nyc_forecast()