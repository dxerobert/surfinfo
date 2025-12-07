import requests
from datetime import datetime, UTC, timedelta
import json

# === YOUR SETTINGS ===
API_KEY = "d6c4d1f4-b04c-11ef-ae24-0242ac130003-d6c4d2a8-b04c-11ef-ae24-0242ac130003"

# Put your surf spot coordinates here 
LAT = 28.04085  # latitude
LNG = -80.33260  # longitude

# Surfline spot ID for Ocean Avenue, Melbourne Beach, FL
SURFLINE_SPOT_ID = "5842041f4e65fad6a7708e1a"

# Function to convert degrees to cardinal direction
def degrees_to_cardinal(degrees):
    """Convert degrees (0-360) to cardinal direction (N, NE, E, SE, S, SW, W, NW)"""
    if degrees is None or degrees == 'N/A':
        return 'N/A'
    try:
        degrees = float(degrees)
        directions = ['N', 'NE', 'E', 'SE', 'S', 'SW', 'W', 'NW']
        index = round(degrees / 45) % 8
        return directions[index]
    except (ValueError, TypeError):
        return 'N/A'

# Function to get data from Stormglass
def get_stormglass_data(lat, lng, start_time, end_time, api_key):
    """Fetch data from Stormglass API"""
    params = [
        "waveHeight", "waveDirection", "wavePeriod",
        "secondarySwellHeight", "secondarySwellDirection", "secondarySwellPeriod",
        "swellHeight", "swellDirection", "swellPeriod",
        "windSpeed", "windDirection",
        "waterTemperature",
        "airTemperature",
        "seaLevel"
    ]
    
    url = "https://api.stormglass.io/v2/weather/point"
    headers = {"Authorization": api_key}
    query_params = {
        "lat": lat,
        "lng": lng,
        "params": ",".join(params),
        "source": "noaa,sg",
        "start": start_time.isoformat(),
        "end": end_time.isoformat()
    }
    
    response = requests.get(url, params=query_params, headers=headers)
    
    # Get tide data
    tide_url = "https://api.stormglass.io/v2/tide/extremes/point"
    tide_params = {
        "lat": lat,
        "lng": lng,
        "start": start_time.isoformat(),
        "end": (end_time + timedelta(days=2)).isoformat()
    }
    tide_response = requests.get(tide_url, params=tide_params, headers=headers)
    
    return response, tide_response

# Function to get data from Open-Meteo (free backup)
def get_openmeteo_data(lat, lng):
    """Fetch marine weather data from Open-Meteo API (free, no API key required)"""
    # Get marine data
    marine_url = "https://marine-api.open-meteo.com/v1/marine"
    marine_params = {
        "latitude": lat,
        "longitude": lng,
        "hourly": "wave_height,wave_direction,wave_period,wind_speed_10m,wind_direction_10m,sea_surface_temperature",
        "timezone": "UTC",
        "forecast_days": 2,
        "past_days": 1
    }
    
    # Get air temperature from regular weather API
    weather_url = "https://api.open-meteo.com/v1/forecast"
    weather_params = {
        "latitude": lat,
        "longitude": lng,
        "hourly": "temperature_2m",
        "timezone": "UTC",
        "forecast_days": 2,
        "past_days": 1
    }
    
    try:
        marine_response = requests.get(marine_url, params=marine_params, timeout=10)
        weather_response = requests.get(weather_url, params=weather_params, timeout=10)
        return marine_response, weather_response
    except Exception as e:
        print(f"Error connecting to Open-Meteo: {e}")
        return None, None

# Function to parse Open-Meteo data
def parse_openmeteo_data(marine_data, weather_data, lat, lng):
    """Parse Open-Meteo response and format it similar to Stormglass format"""
    if marine_data.status_code != 200:
        return None
    
    marine_json = marine_data.json()
    hourly = marine_json.get("hourly", {})
    
    if not hourly or "time" not in hourly:
        return None
    
    times = hourly["time"]
    wave_heights = hourly.get("wave_height", [])
    wave_directions = hourly.get("wave_direction", [])
    wave_periods = hourly.get("wave_period", [])
    wind_speeds = hourly.get("wind_speed_10m", [])
    wind_directions = hourly.get("wind_direction_10m", [])
    water_temps = hourly.get("sea_surface_temperature", [])
    
    # Get air temperature from weather API
    air_temps = []
    if weather_data and weather_data.status_code == 200:
        weather_json = weather_data.json()
        weather_hourly = weather_json.get("hourly", {})
        air_temps = weather_hourly.get("temperature_2m", [])
    
    # Get current (latest) data
    if not times:
        return None
    
    latest_idx = len(times) - 1
    
    # Build hours array in Stormglass-like format
    hours = []
    for i, time_str in enumerate(times):
        hour_data = {
            "time": time_str,
            "swellHeight": {"sg": wave_heights[i] if i < len(wave_heights) else None},
            "swellDirection": {"sg": wave_directions[i] if i < len(wave_directions) else None},
            "swellPeriod": {"sg": wave_periods[i] if i < len(wave_periods) else None},
            "windSpeed": {"sg": wind_speeds[i] if i < len(wind_speeds) else None},
            "windDirection": {"sg": wind_directions[i] if i < len(wind_directions) else None},
            "waterTemperature": {"sg": water_temps[i] if i < len(water_temps) else None},
            "airTemperature": {"sg": air_temps[i] if i < len(air_temps) else None},
        }
        hours.append(hour_data)
    
    return {
        "hours": hours,
        "source": "openmeteo"
    }

# Function to find nearest NOAA tide station
def find_nearest_noaa_station(lat, lng):
    """Find the nearest NOAA tide station to given coordinates"""
    # Common stations for Florida/East Coast and other US coastal areas
    stations = {
        "8721604": {"name": "Trident Pier, Port Canaveral, FL", "lat": 28.4167, "lng": -80.5883},
        "8723214": {"name": "Virginia Key, FL", "lat": 25.7317, "lng": -80.1617},
        "8723970": {"name": "Vaca Key, FL", "lat": 24.7117, "lng": -81.1050},
        "8724580": {"name": "Key West, FL", "lat": 24.5500, "lng": -81.8083},
        "8725520": {"name": "Fort Myers, FL", "lat": 26.6467, "lng": -81.8717},
        "8726384": {"name": "Naples, FL", "lat": 26.1317, "lng": -81.8083},
        "8726520": {"name": "St. Petersburg, FL", "lat": 27.7600, "lng": -82.6267},
        "8726724": {"name": "Clearwater Beach, FL", "lat": 27.9783, "lng": -82.8317},
        "8729108": {"name": "Panama City, FL", "lat": 30.1533, "lng": -85.6667},
        "8735180": {"name": "Dauphin Island, AL", "lat": 30.2500, "lng": -88.0750},
        "8761305": {"name": "Pilottown, LA", "lat": 29.1783, "lng": -89.2583},
        "8770475": {"name": "Port Aransas, TX", "lat": 27.8367, "lng": -97.0467},
        "9410170": {"name": "San Diego, CA", "lat": 32.7150, "lng": -117.1733},
        "9414290": {"name": "Santa Monica, CA", "lat": 34.0083, "lng": -118.5000},
        "9413450": {"name": "Los Angeles, CA", "lat": 33.7383, "lng": -118.2733},
        "9414750": {"name": "Santa Barbara, CA", "lat": 34.4067, "lng": -119.6917},
        "9415144": {"name": "Port San Luis, CA", "lat": 35.1683, "lng": -120.7600},
        "9416841": {"name": "Monterey, CA", "lat": 36.6050, "lng": -121.8883},
        "9414290": {"name": "San Francisco, CA", "lat": 37.8067, "lng": -122.4650},
        "9432780": {"name": "Astoria, OR", "lat": 46.2083, "lng": -123.7683},
        "9447130": {"name": "Seattle, WA", "lat": 47.6067, "lng": -122.3383},
        "8443970": {"name": "Boston, MA", "lat": 42.3533, "lng": -71.0500},
        "8531680": {"name": "Sandy Hook, NJ", "lat": 40.4667, "lng": -74.0083},
        "8518750": {"name": "The Battery, NY", "lat": 40.7000, "lng": -74.0167},
        "8651370": {"name": "Duck, NC", "lat": 36.1833, "lng": -75.7467},
        "8661070": {"name": "Charleston, SC", "lat": 32.7817, "lng": -79.9250},
        "8670870": {"name": "Fort Pulaski, GA", "lat": 32.0333, "lng": -80.9017},
    }
    
    # Find closest station
    min_dist = float('inf')
    closest_station_id = None
    closest_station_info = None
    
    for station_id, station_info in stations.items():
        # Simple distance calculation (Haversine would be better but this works for nearby stations)
        dist = ((lat - station_info["lat"])**2 + (lng - station_info["lng"])**2)**0.5
        if dist < min_dist:
            min_dist = dist
            closest_station_id = station_id
            closest_station_info = station_info.copy()
            closest_station_info["id"] = station_id
            closest_station_info["distance"] = dist
    
    if closest_station_id:
        return closest_station_info
    return None

# Function to get NOAA tide data
def get_noaa_tide_data(lat, lng):
    """Fetch tide predictions from NOAA CO-OPS API"""
    station_info = find_nearest_noaa_station(lat, lng)
    if not station_info:
        return None, None, None
    
    station_id = station_info["id"]
    
    # Get date range for next 48 hours
    now = datetime.now(UTC)
    begin_date = now.strftime("%Y%m%d")
    end_date = (now + timedelta(days=2)).strftime("%Y%m%d")
    
    # Try to get high/low tide predictions first (more accurate)
    url = f"https://api.tidesandcurrents.noaa.gov/api/prod/datagetter"
    params = {
        "product": "predictions",
        "application": "NOS.COOPS.TAC.WL",
        "datum": "MLLW",
        "station": station_id,
        "begin_date": begin_date,
        "end_date": end_date,
        "time_zone": "gmt",
        "units": "metric",
        "interval": "hilo",  # Get high/low predictions
        "format": "json"
    }
    
    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            # Check for errors
            if "error" in data:
                # Try hourly predictions instead
                params["interval"] = "h"
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    data = response.json()
                    if "error" not in data:
                        return data, station_info
            elif "predictions" in data:
                return data, station_info
        
        # Fallback to hourly predictions if hilo doesn't work
        params["interval"] = "h"
        response = requests.get(url, params=params, timeout=10)
        if response.status_code == 200:
            data = response.json()
            if "error" not in data:
                return data, station_info
        return None, None, None
    except Exception as e:
        print(f"Error fetching NOAA tide data: {e}")
        return None, None, None

# Function to get NOAA wind data from NDBC
def get_noaa_wind_data(lat, lng):
    """Fetch wind data from NOAA NDBC (National Data Buoy Center)"""
    # Common NDBC buoys for US coastal areas
    buoys = {
        "41009": {"name": "CANAVERAL 20 NM East of Cape Canaveral, FL", "lat": 28.5, "lng": -80.18},
        "41008": {"name": "GRAYS REEF 50 NM Southeast of Savannah, GA", "lat": 31.4, "lng": -80.87},
        "41010": {"name": "12 NM East of Frying Pan Shoals, NC", "lat": 33.4, "lng": -77.5},
        "41013": {"name": "Frying Pan Shoals, NC", "lat": 33.4, "lng": -77.5},
        "41025": {"name": "Diamond Shoals, NC", "lat": 35.0, "lng": -75.4},
        "44008": {"name": "Nantucket Sound, MA", "lat": 41.3, "lng": -70.2},
        "44013": {"name": "Boston, MA", "lat": 42.3, "lng": -70.6},
        "46042": {"name": "Monterey Bay, CA", "lat": 36.8, "lng": -122.0},
        "46026": {"name": "San Francisco, CA", "lat": 37.8, "lng": -122.8},
        "46059": {"name": "Santa Monica Basin, CA", "lat": 33.7, "lng": -118.4},
        "46086": {"name": "San Pedro, CA", "lat": 33.7, "lng": -118.2},
        "46214": {"name": "Half Moon Bay, CA", "lat": 37.4, "lng": -122.9},
    }
    
    # Find closest buoy
    min_dist = float('inf')
    closest_buoy_id = None
    closest_buoy_info = None
    
    for buoy_id, buoy_info in buoys.items():
        dist = ((lat - buoy_info["lat"])**2 + (lng - buoy_info["lng"])**2)**0.5
        if dist < min_dist:
            min_dist = dist
            closest_buoy_id = buoy_id
            closest_buoy_info = buoy_info.copy()
            closest_buoy_info["id"] = buoy_id
            closest_buoy_info["distance"] = dist
    
    if not closest_buoy_id:
        return None, None
    
    # Get latest observations from NDBC
    url = f"https://www.ndbc.noaa.gov/data/realtime2/{closest_buoy_id}.txt"
    
    try:
        response = requests.get(url, timeout=10)
        if response.status_code == 200:
            # Parse NDBC text format
            lines = response.text.strip().split('\n')
            if len(lines) > 2:
                # Skip header lines, get latest data
                data_line = lines[-1].split()
                if len(data_line) >= 10:
                    # NDBC format: YY MM DD hh mm WDIR WSPD GDR GST
                    # WDIR is wind direction, WSPD is wind speed (m/s)
                    wind_dir = data_line[5] if data_line[5] != 'MM' else None
                    wind_speed = data_line[6] if data_line[6] != 'MM' else None
                    wind_data = {
                        "windSpeed": float(wind_speed) if wind_speed else None,
                        "windDirection": float(wind_dir) if wind_dir else None
                    }
                    return wind_data, closest_buoy_info
        return None, None
    except Exception as e:
        print(f"Error fetching NOAA wind data: {e}")
        return None, None

# Function to parse NOAA tide data
def parse_noaa_tide_data(tide_json, station_info):
    """Parse NOAA tide predictions and extract high/low tides"""
    if not tide_json or "predictions" not in tide_json:
        return [], []
    
    predictions = tide_json["predictions"]
    high_tides = []
    low_tides = []
    now_dt = datetime.now(UTC)
    
    for pred in predictions:
        if "t" not in pred or "v" not in pred:
            continue
        
        tide_time_str = pred["t"]
        # Handle different time formats from NOAA
        if 'T' in tide_time_str:
            tide_time = datetime.fromisoformat(tide_time_str.replace('Z', '+00:00'))
        else:
            # Format: YYYY-MM-DD HH:MM
            tide_time = datetime.strptime(tide_time_str, "%Y-%m-%d %H:%M")
            tide_time = tide_time.replace(tzinfo=UTC)
        
        tide_height = float(pred["v"])
        
        # Only future tides
        if tide_time > now_dt:
            # Check if this is marked as high or low (hilo format)
            if "type" in pred:
                tide_type = pred["type"].lower()
                if tide_type == "h" or tide_type == "high":
                    high_tides.append({"time": pred["t"], "height": tide_height, "type": "high"})
                elif tide_type == "l" or tide_type == "low":
                    low_tides.append({"time": pred["t"], "height": tide_height, "type": "low"})
            else:
                # Fallback: determine by comparing with surrounding values
                # This is less accurate but works for hourly data
                idx = predictions.index(pred)
                if idx > 0 and idx < len(predictions) - 1:
                    prev_height = float(predictions[idx - 1]["v"])
                    next_height = float(predictions[idx + 1]["v"]) if idx + 1 < len(predictions) else prev_height
                    if tide_height > prev_height and tide_height > next_height:
                        high_tides.append({"time": pred["t"], "height": tide_height, "type": "high"})
                    elif tide_height < prev_height and tide_height < next_height:
                        low_tides.append({"time": pred["t"], "height": tide_height, "type": "low"})
    
    return high_tides, low_tides

# Function to get temperature data from alternative sources
def get_temperature_backup(lat, lng, api_key=None):
    """Fetch air and water temperature from backup sources (Stormglass or Open-Meteo)"""
    # Try Stormglass first (most reliable for marine data)
    if api_key:
        try:
            now = datetime.now(UTC)
            start_time = now - timedelta(hours=1)
            url = "https://api.stormglass.io/v2/weather/point"
            headers = {"Authorization": api_key}
            params = {
                "lat": lat,
                "lng": lng,
                "params": "waterTemperature,airTemperature",
                "source": "noaa,sg",
                "start": start_time.isoformat(),
                "end": now.isoformat()
            }
            response = requests.get(url, params=params, headers=headers, timeout=10)
            if response.status_code == 200:
                data = response.json()
                hours = data.get("hours", [])
                if hours:
                    latest = hours[-1]
                    water_temp = latest.get("waterTemperature", {})
                    air_temp = latest.get("airTemperature", {})
                    water_c = water_temp.get("noaa") or water_temp.get("sg")
                    air_c = air_temp.get("noaa") or air_temp.get("sg")
                    return {
                        "waterTemperature": water_c,
                        "airTemperature": air_c,
                        "source": "stormglass"
                    }
        except Exception as e:
            print(f"  (Stormglass temperature backup failed: {e})")
    
    # Fallback to Open-Meteo
    try:
        marine_url = "https://marine-api.open-meteo.com/v1/marine"
        marine_params = {
            "latitude": lat,
            "longitude": lng,
            "hourly": "sea_surface_temperature",
            "timezone": "UTC",
            "forecast_days": 1,
            "temperature_unit": "celsius"  # Open-Meteo returns Celsius
        }
        weather_url = "https://api.open-meteo.com/v1/forecast"
        weather_params = {
            "latitude": lat,
            "longitude": lng,
            "hourly": "temperature_2m",
            "timezone": "UTC",
            "forecast_days": 1,
            "temperature_unit": "celsius"  # Open-Meteo returns Celsius
        }
        
        marine_response = requests.get(marine_url, params=marine_params, timeout=10)
        weather_response = requests.get(weather_url, params=weather_params, timeout=10)
        
        if marine_response.status_code == 200 and weather_response.status_code == 200:
            marine_json = marine_response.json()
            weather_json = weather_response.json()
            
            marine_hourly = marine_json.get("hourly", {})
            weather_hourly = weather_json.get("hourly", {})
            
            water_temps = marine_hourly.get("sea_surface_temperature", [])
            air_temps = weather_hourly.get("temperature_2m", [])
            
            if water_temps and air_temps:
                # Open-Meteo returns in Celsius, keep as Celsius for conversion later
                return {
                    "waterTemperature": water_temps[-1] if water_temps else None,
                    "airTemperature": air_temps[-1] if air_temps else None,
                    "source": "openmeteo"
                }
    except Exception as e:
        print(f"  (Open-Meteo temperature backup failed: {e})")
    
    return None

# Function to get Surfline data
def get_surfline_data(spot_id):
    """Fetch surf data from Surfline API"""
    # Surfline API endpoints
    forecast_url = f"https://services.surfline.com/kbyg/regions/forecasts/conditions"
    wave_url = f"https://services.surfline.com/kbyg/spots/forecasts/wave"
    wind_url = f"https://services.surfline.com/kbyg/spots/forecasts/wind"
    tide_url = f"https://services.surfline.com/kbyg/spots/forecasts/tides"
    rating_url = f"https://services.surfline.com/kbyg/spots/forecasts/rating"
    
    params = {
        "spotId": spot_id,
        "days": 2,
        "intervalHours": 1
    }
    
    try:
        # Get wave forecast
        wave_response = requests.get(wave_url, params=params, timeout=10)
        # Get wind forecast
        wind_response = requests.get(wind_url, params=params, timeout=10)
        # Get tide forecast
        tide_response = requests.get(tide_url, params=params, timeout=10)
        # Get conditions
        conditions_response = requests.get(forecast_url, params={"spotId": spot_id}, timeout=10)
        # Get rating
        rating_response = requests.get(rating_url, params={"spotId": spot_id, "days": 2}, timeout=10)
        
        return {
            "wave": wave_response.json() if wave_response.status_code == 200 else None,
            "wind": wind_response.json() if wind_response.status_code == 200 else None,
            "tide": tide_response.json() if tide_response.status_code == 200 else None,
            "conditions": conditions_response.json() if conditions_response.status_code == 200 else None,
            "rating": rating_response.json() if rating_response.status_code == 200 else None
        }
    except Exception as e:
        print(f"Error fetching Surfline data: {e}")
        return None

# Function to parse Surfline data
def parse_surfline_data(surfline_data, spot_id):
    """Parse Surfline response and format it similar to Stormglass format"""
    if not surfline_data:
        return None
    
    hours = []
    now_dt = datetime.now(UTC)
    
    # Parse wave data
    wave_data = surfline_data.get("wave", {})
    if wave_data and "data" in wave_data:
        wave_points = wave_data["data"].get("wave", [])
        
        # Parse wind data
        wind_data = surfline_data.get("wind", {})
        wind_points = []
        if wind_data and "data" in wind_data:
            wind_points = wind_data["data"].get("wind", [])
        
        # Parse tide data
        tide_data = surfline_data.get("tide", {})
        tide_points = []
        if tide_data and "data" in tide_data:
            tide_points = tide_data["data"].get("tides", [])
        
        # Parse rating data
        rating_data = surfline_data.get("rating", {})
        rating_points = []
        if rating_data and "data" in rating_data:
            rating_points = rating_data["data"].get("rating", [])
        
        # Create a dictionary of ratings by timestamp for quick lookup
        rating_dict = {}
        for rating_point in rating_points:
            timestamp = rating_point.get("timestamp")
            if timestamp:
                rating_dict[timestamp] = rating_point.get("rating", {})
        
        # Combine data by timestamp
        for i, wave_point in enumerate(wave_points):
            timestamp = wave_point.get("timestamp")
            if not timestamp:
                continue
            
            # Convert timestamp to ISO format
            time_str = datetime.fromtimestamp(timestamp, tz=UTC).isoformat()
            
            # Get corresponding wind data
            wind_point = wind_points[i] if i < len(wind_points) else {}
            
            # Get rating for this timestamp
            rating_info = rating_dict.get(timestamp, {})
            
            # Build hour data
            hour_data = {
                "time": time_str,
                "swellHeight": {"sg": wave_point.get("surf", {}).get("min")},  # Use min surf height
                "swellDirection": {"sg": wave_point.get("swells", [{}])[0].get("direction") if wave_point.get("swells") else None},
                "swellPeriod": {"sg": wave_point.get("swells", [{}])[0].get("period") if wave_point.get("swells") else None},
                "windSpeed": {"sg": wind_point.get("speed")},  # Already in m/s
                "windDirection": {"sg": wind_point.get("direction")},
                "waterTemperature": {"sg": wave_point.get("temperature")},
                "airTemperature": {"sg": None},  # Not always available in wave data
                "rating": rating_info,  # Store rating info
            }
            hours.append(hour_data)
    
    if not hours:
        return None
    
    return {
        "hours": hours,
        "source": "surfline",
        "spot_id": spot_id
    }

# Function to display surf report
def display_surf_report(data, tide_data=None, source="stormglass", lat=None, lng=None, sources=None, rating=None):
    """Display formatted surf report from combined data sources"""
    hours = data.get("hours", [])
    if not hours:
        print("No data available")
        return
    
    latest = hours[-1]
    now_dt = datetime.fromisoformat(latest['time'].replace('Z', '+00:00'))
    
    # Find data from 6 hours ago and 24 hours ago
    six_hours_ago = None
    twenty_four_hours_ago = None
    
    for hour_data in hours:
        hour_dt = datetime.fromisoformat(hour_data['time'].replace('Z', '+00:00'))
        hours_diff = (now_dt - hour_dt).total_seconds() / 3600
        
        if 5.5 <= hours_diff <= 6.5 and six_hours_ago is None:
            six_hours_ago = hour_data
        if 23.5 <= hours_diff <= 24.5 and twenty_four_hours_ago is None:
            twenty_four_hours_ago = hour_data
    
    # Determine location name
    if SURFLINE_SPOT_ID:
        spot_name = "Ocean Avenue, Melbourne Beach, FL"
        location_display = spot_name
    else:
        location_display = f"{LAT}, {LNG}"
    
    print(f"🌊 Current Surf Report for {location_display}")
    print(f"Time (UTC): {latest['time']}\n")
    
    # Display Surfline star rating if available (always show if we have it)
    if rating:
        rating_value = rating.get("value", 0)
        rating_key = rating.get("key", "N/A")
        # Display stars (1-5 scale)
        stars = "⭐" * rating_value
        # Format rating key nicely (e.g., "FAIR_TO_GOOD" -> "Fair to Good")
        rating_display = rating_key.replace("_", " ").title() if rating_key != "N/A" else "N/A"
        print(f"Surfline Rating      : {stars} ({rating_display}, {rating_value}/5)")
        print()
    
    # Show data sources if using combined sources
    if sources:
        source_info = []
        if sources.get("swell"):
            source_info.append(f"Swell: {sources['swell'].title()}")
        if sources.get("wind"):
            source_info.append(f"Wind: {sources['wind'].title()}")
        if sources.get("air_temp") or sources.get("water_temp"):
            temp_source = sources.get("air_temp") or sources.get("water_temp")
            source_info.append(f"Temp: {temp_source.title()}")
        if source_info:
            print(f"Data Sources         : {', '.join(source_info)}")
            print()
    
    # Current Primary Swell (convert meters to feet)
    swell_height = latest.get('swellHeight', {}).get('noaa') or latest.get('swellHeight', {}).get('sg')
    if swell_height is not None:
        swell_height_ft = swell_height * 3.28084
        swell_period = latest.get('swellPeriod', {}).get('noaa') or latest.get('swellPeriod', {}).get('sg')
        swell_dir_deg = latest.get('swellDirection', {}).get('noaa') or latest.get('swellDirection', {}).get('sg')
        swell_dir_card = degrees_to_cardinal(swell_dir_deg)
        if swell_period is not None and swell_period != 'N/A':
            print(f"Primary Swell Right Now       : {swell_height_ft:.1f} ft "
                  f"@ {swell_period:.1f} s "
                  f"{swell_dir_deg:.1f}° ({swell_dir_card})")
        else:
            print(f"Primary Swell Right Now       : {swell_height_ft:.1f} ft "
                  f"@ N/A s "
                  f"{swell_dir_deg:.1f}° ({swell_dir_card})")
    else:
        print(f"Primary Swell       : N/A")
    
    # Secondary Swell (only available from Stormglass)
    if source == "stormglass":
        sec_swell_height = latest.get('secondarySwellHeight', {}).get('noaa')
        if sec_swell_height is not None:
            sec_swell_height_ft = sec_swell_height * 3.28084
            sec_swell_period = latest.get('secondarySwellPeriod', {}).get('noaa', 'N/A')
            sec_swell_dir_deg = latest.get('secondarySwellDirection', {}).get('noaa', 'N/A')
            sec_swell_dir_card = degrees_to_cardinal(sec_swell_dir_deg)
            print(f"Secondary Swell     : {sec_swell_height_ft:.1f} ft "
                  f"@ {sec_swell_period} s "
                  f"{sec_swell_dir_deg}° ({sec_swell_dir_card})")
        else:
            print(f"Secondary Swell     : N/A")
    
    # Swell from 6 hours ago
    if six_hours_ago:
        past_swell = six_hours_ago.get('swellHeight', {}).get('noaa') or six_hours_ago.get('swellHeight', {}).get('sg')
        if past_swell is not None:
            past_swell_ft = past_swell * 3.28084
            past_period = six_hours_ago.get('swellPeriod', {}).get('noaa') or six_hours_ago.get('swellPeriod', {}).get('sg')
            past_dir_deg = six_hours_ago.get('swellDirection', {}).get('noaa') or six_hours_ago.get('swellDirection', {}).get('sg')
            past_dir_card = degrees_to_cardinal(past_dir_deg)
            if past_period is not None and past_period != 'N/A':
                print(f"Recent Swell (6h ago)      : {past_swell_ft:.1f} ft "
                      f"@ {past_period:.1f} s "
                      f"{past_dir_deg:.1f}° ({past_dir_card})")
            else:
                print(f"Recent Swell (6h ago)      : {past_swell_ft:.1f} ft "
                      f"@ N/A s "
                      f"{past_dir_deg:.1f}° ({past_dir_card})")
    
    # Swell from 24 hours ago
    if twenty_four_hours_ago:
        past_swell_24 = twenty_four_hours_ago.get('swellHeight', {}).get('noaa') or twenty_four_hours_ago.get('swellHeight', {}).get('sg')
        if past_swell_24 is not None:
            past_swell_24_ft = past_swell_24 * 3.28084
            past_period_24 = twenty_four_hours_ago.get('swellPeriod', {}).get('noaa') or twenty_four_hours_ago.get('swellPeriod', {}).get('sg')
            past_dir_24_deg = twenty_four_hours_ago.get('swellDirection', {}).get('noaa') or twenty_four_hours_ago.get('swellDirection', {}).get('sg')
            past_dir_24_card = degrees_to_cardinal(past_dir_24_deg)
            if past_period_24 is not None and past_period_24 != 'N/A':
                print(f"Yesterday's Swell (24h ago)     : {past_swell_24_ft:.1f} ft "
                      f"@ {past_period_24:.1f} s "
                      f"{past_dir_24_deg:.1f}° ({past_dir_24_card})")
            else:
                print(f"Yesterday's Swell (24h ago)     : {past_swell_24_ft:.1f} ft "
                      f"@ N/A s "
                      f"{past_dir_24_deg:.1f}° ({past_dir_24_card})")
    
    print()  # blank line
    
    # Wind (convert m/s to mph)
    wind_speed = latest.get('windSpeed', {}).get('noaa') or latest.get('windSpeed', {}).get('sg')
    wind_dir_deg = latest.get('windDirection', {}).get('noaa') or latest.get('windDirection', {}).get('sg')
    
    # If wind data is missing and we have coordinates, try NOAA backup
    noaa_wind_station = None
    if (wind_speed is None or wind_dir_deg is None) and lat and lng:
        print("  (Fetching wind data from NOAA backup...)")
        noaa_wind, noaa_wind_station = get_noaa_wind_data(lat, lng)
        if noaa_wind:
            if wind_speed is None:
                wind_speed = noaa_wind.get("windSpeed")
            if wind_dir_deg is None:
                wind_dir_deg = noaa_wind.get("windDirection")
    
    if wind_speed is not None:
        wind_speed_mph = wind_speed * 2.237
        wind_dir_card = degrees_to_cardinal(wind_dir_deg)
        wind_source = " (NOAA backup)" if source == "openmeteo" and lat and lng else ""
        print(f"Wind                : {wind_speed_mph:.1f} mph "
              f"from {wind_dir_deg:.1f}° ({wind_dir_card}){wind_source}")
        if noaa_wind_station:
            print(f"  └─ Station: {noaa_wind_station['name']} (ID: {noaa_wind_station['id']}, "
                  f"Lat: {noaa_wind_station['lat']:.3f}°, Lng: {noaa_wind_station['lng']:.3f}°)")
    else:
        print(f"Wind                : N/A")
    
    # Air temperature (convert Celsius to Fahrenheit)
    air_temp_source = latest.get("airTemperature", {})
    air_temp_c = air_temp_source.get("noaa") or air_temp_source.get("sg")
    air_temp_backup_source = None
    temp_backup = None
    
    # If air temp is missing and we have coordinates, try backup sources
    # Skip if we're using combined sources and temp was already fetched
    if air_temp_c is None and lat and lng and source != "combined":
        if source == "surfline":
            print("  (Fetching temperature data from backup source...)")
        temp_backup = get_temperature_backup(lat, lng, API_KEY if 'API_KEY' in globals() else None)
        if temp_backup:
            if air_temp_c is None:
                air_temp_c = temp_backup.get("airTemperature")
                air_temp_backup_source = temp_backup.get("source")
    elif source == "combined" and sources:
        # Use the source info from combined fetch
        air_temp_backup_source = sources.get("air_temp")
    
    if air_temp_c is not None:
        # Open-Meteo returns Celsius, always convert from Celsius to Fahrenheit
        air_temp_f = (air_temp_c * 9/5) + 32  # Convert from Celsius
        temp_source_label = f" ({air_temp_backup_source})" if air_temp_backup_source and source != "combined" else ""
        print(f"Air Temperature     : {air_temp_f:.1f} °F{temp_source_label}")
    else:
        print(f"Air Temperature     : N/A")
    
    # Water temperature (convert Celsius to Fahrenheit)
    temp_source = latest.get("waterTemperature", {})
    temp_c = temp_source.get("noaa") or temp_source.get("sg")
    water_temp_backup_source = None
    
    # If water temp is missing and we have coordinates, try backup sources
    # Skip if we're using combined sources and temp was already fetched
    if temp_c is None and lat and lng and source != "combined":
        # Reuse the backup data we already fetched if available
        if temp_backup:
            temp_c = temp_backup.get("waterTemperature")
            water_temp_backup_source = temp_backup.get("source")
        else:
            # Fetch if we didn't already fetch for air temp
            if source == "surfline":
                print("  (Fetching temperature data from backup source...)")
            temp_backup = get_temperature_backup(lat, lng, API_KEY if 'API_KEY' in globals() else None)
            if temp_backup:
                temp_c = temp_backup.get("waterTemperature")
                water_temp_backup_source = temp_backup.get("source")
    elif source == "combined" and sources:
        # Use the source info from combined fetch
        water_temp_backup_source = sources.get("water_temp")
    
    if temp_c is not None:
        # Open-Meteo returns Celsius, always convert from Celsius to Fahrenheit
        temp_f = (temp_c * 9/5) + 32  # Convert from Celsius
        temp_source_label = f" ({water_temp_backup_source})" if water_temp_backup_source and source != "combined" else ""
        print(f"Water Temperature   : {temp_f:.1f} °F{temp_source_label}")
    else:
        print(f"Water Temperature   : N/A")
    
    # Sea Level (only from Stormglass, convert meters to feet)
    if source == "stormglass":
        sea_level = latest.get("seaLevel", {})
        level = sea_level.get("noaa") or sea_level.get("sg")
        if level is not None:
            level_ft = level * 3.28084
            print(f"Sea Level           : {level_ft:.2f} ft")
    
    # Tide information
    print("\n" + "="*50)
    print("TIDE INFORMATION")
    print("="*50)
    
    high_tides = []
    low_tides = []
    
    # Get tide data from Stormglass if available
    if tide_data and tide_data.status_code == 200:
        tide_json = tide_data.json()
        tide_extremes = tide_json.get("data", [])
        
        for tide_event in tide_extremes:
            tide_time = datetime.fromisoformat(tide_event['time'].replace('Z', '+00:00'))
            if tide_time > now_dt:  # Only future tides
                if tide_event["type"] == "high":
                    high_tides.append(tide_event)
                elif tide_event["type"] == "low":
                    low_tides.append(tide_event)
    
    # Fallback: Look for tide data in the hours array
    if not high_tides and not low_tides:
        for hour_data in hours:
            if "tide" in hour_data:
                for tide_event in hour_data["tide"]:
                    tide_time = datetime.fromisoformat(tide_event['time'].replace('Z', '+00:00'))
                    if tide_time > now_dt:  # Only future tides
                        if tide_eventCurrent["type"] == "high":
                            high_tides.append(tide_event)
                        elif tide_event["type"] == "low":
                            low_tides.append(tide_event)
    
    # Fallback to NOAA if no tide data available
    noaa_tide_station = None
    if (not high_tides or not low_tides) and lat and lng:
        print("  (Fetching tide data from NOAA backup...)")
        noaa_tide_json, noaa_tide_station = get_noaa_tide_data(lat, lng)
        if noaa_tide_json and noaa_tide_station:
            noaa_high, noaa_low = parse_noaa_tide_data(noaa_tide_json, noaa_tide_station)
            if not high_tides and noaa_high:
                high_tides = noaa_high[:1]  # Just get the next one
            if not low_tides and noaa_low:
                low_tides = noaa_low[:1]  # Just get the next one
    
    # Sort and get next high and low tides
    high_tides.sort(key=lambda x: x['time'])
    low_tides.sort(key=lambda x: x['time'])
    
    if high_tides:
        next_high = high_tides[0]
        tide_time = datetime.fromisoformat(next_high['time'].replace('Z', '+00:00'))
        time_str = tide_time.strftime("%I:%M %p")
        tide_height = next_high.get('height', 0)
        tide_height_ft = tide_height * 3.28084 if tide_height else 0
        tide_source = " (NOAA)" if source == "openmeteo" and lat and lng else ""
        print(f"Next High Tide      : {time_str} ({tide_height_ft:.2f} ft){tide_source}")
    else:
        print(f"Next High Tide      : N/A")
    
    if low_tides:
        next_low = low_tides[0]
        tide_time = datetime.fromisoformat(next_low['time'].replace('Z', '+00:00'))
        time_str = tide_time.strftime("%I:%M %p")
        tide_height = next_low.get('height', 0)
        tide_height_ft = tide_height * 3.28084 if tide_height else 0
        tide_source = " (NOAA)" if source == "openmeteo" and lat and lng else ""
        print(f"Next Low Tide       : {time_str} ({tide_height_ft:.2f} ft){tide_source}")
    else:
        print(f"Next Low Tide       : N/A")
    
    # Display station information
    if noaa_tide_station:
        print(f"\nTide Station        : {noaa_tide_station['name']}")
        print(f"  └─ Station ID: {noaa_tide_station['id']}, "
              f"Location: {noaa_tide_station['lat']:.3f}°, {noaa_tide_station['lng']:.3f}°")

# Function to fetch and combine data from multiple sources with priority
def fetch_combined_surf_data(lat, lng, api_key, surfline_spot_id):
    """Fetch data from all sources and combine based on priority"""
    now = datetime.now(UTC)
    start_time = now - timedelta(hours=24)
    
    combined_data = {
        "hours": [],
        "rating": None,
        "sources": {
            "swell": None,
            "wind": None,
            "air_temp": None,
            "water_temp": None,
            "rating": None
        }
    }
    
    # 1. Always fetch Surfline rating first
    print("Fetching Surfline rating...")
    surfline_rating_data = None
    if surfline_spot_id:
        try:
            rating_url = f"https://services.surfline.com/kbyg/spots/forecasts/rating"
            rating_response = requests.get(rating_url, params={"spotId": surfline_spot_id, "days": 2}, timeout=10)
            if rating_response.status_code == 200:
                rating_json = rating_response.json()
                if rating_json and "data" in rating_json:
                    rating_points = rating_json["data"].get("rating", [])
                    if rating_points:
                        # Get latest rating
                        latest_rating = rating_points[-1]
                        combined_data["rating"] = latest_rating.get("rating", {})
                        combined_data["sources"]["rating"] = "surfline"
                        surfline_rating_data = rating_json
        except Exception as e:
            print(f"  (Surfline rating failed: {e})")
    
    # 2. Fetch swell data: Stormglass → Open-Meteo → Surfline
    print("Fetching swell data (priority: Stormglass → Open-Meteo → Surfline)...")
    swell_data = None
    swell_source = None
    
    # Try Stormglass first
    try:
        response, _ = get_stormglass_data(lat, lng, start_time, now, api_key)
        if response.status_code == 200:
            data = response.json()
            hours = data.get("hours", [])
            if hours:
                swell_data = hours
                swell_source = "stormglass"
                print("  ✓ Swell data from Stormglass")
    except Exception as e:
        print(f"  (Stormglass failed: {e})")
    
    # Try Open-Meteo if Stormglass failed
    if not swell_data:
        try:
            marine_response, _ = get_openmeteo_data(lat, lng)
            if marine_response and marine_response.status_code == 200:
                parsed = parse_openmeteo_data(marine_response, None, lat, lng)
                if parsed and parsed.get("hours"):
                    swell_data = parsed["hours"]
                    swell_source = "openmeteo"
                    print("  ✓ Swell data from Open-Meteo")
        except Exception as e:
            print(f"  (Open-Meteo failed: {e})")
    
    # Try Surfline as last resort
    if not swell_data and surfline_spot_id:
        try:
            surfline_full = get_surfline_data(surfline_spot_id)
            if surfline_full:
                parsed = parse_surfline_data(surfline_full, surfline_spot_id)
                if parsed and parsed.get("hours"):
                    swell_data = parsed["hours"]
                    swell_source = "surfline"
                    print("  ✓ Swell data from Surfline")
        except Exception as e:
            print(f"  (Surfline swell failed: {e})")
    
    if not swell_data:
        print("  ✗ No swell data available from any source")
        return None
    
    # 3. Fetch wind data: Best available (Stormglass → Open-Meteo → Surfline)
    print("Fetching wind data (best available source)...")
    wind_data = None
    wind_source = None
    
    # Try Stormglass first
    try:
        response, _ = get_stormglass_data(lat, lng, start_time, now, api_key)
        if response.status_code == 200:
            data = response.json()
            hours = data.get("hours", [])
            if hours and hours[-1].get("windSpeed"):
                wind_data = hours
                wind_source = "stormglass"
                print("  ✓ Wind data from Stormglass")
    except:
        pass
    
    # Try Open-Meteo if Stormglass failed
    if not wind_data:
        try:
            marine_response, _ = get_openmeteo_data(lat, lng)
            if marine_response and marine_response.status_code == 200:
                parsed = parse_openmeteo_data(marine_response, None, lat, lng)
                if parsed and parsed.get("hours"):
                    latest = parsed["hours"][-1]
                    if latest.get("windSpeed", {}).get("sg"):
                        wind_data = parsed["hours"]
                        wind_source = "openmeteo"
                        print("  ✓ Wind data from Open-Meteo")
        except:
            pass
    
    # Try Surfline if others failed
    if not wind_data and surfline_spot_id:
        try:
            surfline_full = get_surfline_data(surfline_spot_id)
            if surfline_full:
                parsed = parse_surfline_data(surfline_full, surfline_spot_id)
                if parsed and parsed.get("hours"):
                    latest = parsed["hours"][-1]
                    if latest.get("windSpeed", {}).get("sg"):
                        wind_data = parsed["hours"]
                        wind_source = "surfline"
                        print("  ✓ Wind data from Surfline")
        except:
            pass
    
    # 4. Fetch temperature data: Best available (Stormglass → Open-Meteo → Surfline)
    print("Fetching temperature data (best available source)...")
    temp_backup = get_temperature_backup(lat, lng, api_key)
    if temp_backup:
        combined_data["sources"]["air_temp"] = temp_backup.get("source")
        combined_data["sources"]["water_temp"] = temp_backup.get("source")
        print(f"  ✓ Temperature data from {temp_backup.get('source')}")
    
    # Combine all data into hours array
    # Use swell data as base, then overlay wind and temp from best sources
    combined_data["hours"] = swell_data.copy()
    combined_data["sources"]["swell"] = swell_source
    combined_data["sources"]["wind"] = wind_source
    
    # Overlay wind data if from different source
    if wind_data and wind_source and wind_source != swell_source:
        wind_dict = {}
        for hour in wind_data:
            time_key = hour.get("time")
            if time_key:
                wind_dict[time_key] = hour
        
        for hour in combined_data["hours"]:
            time_key = hour.get("time")
            if time_key in wind_dict:
                wind_hour = wind_dict[time_key]
                # Overlay wind data, preserving existing structure
                existing_wind_speed = hour.get("windSpeed", {})
                existing_wind_dir = hour.get("windDirection", {})
                hour["windSpeed"] = wind_hour.get("windSpeed", existing_wind_speed)
                hour["windDirection"] = wind_hour.get("windDirection", existing_wind_dir)
    
    # Overlay temperature data
    if temp_backup:
        for hour in combined_data["hours"]:
            if not hour.get("airTemperature", {}).get("sg") and not hour.get("airTemperature", {}).get("noaa"):
                hour["airTemperature"] = {"sg": temp_backup.get("airTemperature")}
            if not hour.get("waterTemperature", {}).get("sg") and not hour.get("waterTemperature", {}).get("noaa"):
                hour["waterTemperature"] = {"sg": temp_backup.get("waterTemperature")}
    
    return combined_data

# Main execution
print("="*60)
print("🌊 SURF REPORT - Fetching data from multiple sources...")
print("="*60)
print()

# Fetch combined data from all sources with priority
combined_data = fetch_combined_surf_data(LAT, LNG, API_KEY, SURFLINE_SPOT_ID)

if combined_data and combined_data.get("hours"):
    # Get tide data from Stormglass if available
    now = datetime.now(UTC)
    start_time = now - timedelta(hours=24)
    _, tide_response = get_stormglass_data(LAT, LNG, start_time, now, API_KEY)
    
    # Display the combined report
    display_surf_report(combined_data, tide_response, source="combined", lat=LAT, lng=LNG, sources=combined_data.get("sources"), rating=combined_data.get("rating"))
else:
    print("Error: Could not fetch data from any source")
