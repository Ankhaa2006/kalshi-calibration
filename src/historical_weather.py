import requests
from datetime import date, timedelta

def get_nyc_actual_highs(start_date="2024-01-01", end_date="2025-12-31"):
    """
    Open-Meteo historical API — actual recorded highs for NYC
    No API key needed
    """
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 40.7789,
        "longitude": -73.9692,
        "start_date": start_date,
        "end_date": end_date,
        "daily": "temperature_2m_max",
        "temperature_unit": "fahrenheit",
        "timezone": "America/New_York"
    }
    
    r = requests.get(url, params=params)
    data = r.json()
    
    dates = data["daily"]["time"]
    highs = data["daily"]["temperature_2m_max"]
    
    print(f"Fetched {len(dates)} days of NYC high temps\n")
    print(f"{'Date':<15} {'Actual High':>12}")
    print("-" * 30)
    for d, h in zip(dates[-10:], highs[-10:]):  # show last 10
        print(f"{d:<15} {h:>10.1f}°F")
    
    return dict(zip(dates, highs))

actuals = get_nyc_actual_highs()