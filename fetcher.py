import time
import requests
import logging
from database import get_db_connection

logging.basicConfig(level=logging.INFO)

INFO_API_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
STATUS_API_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"

# We fix exactly 10 high-capacity stations so they remain consistent across runs
TARGET_STATIONS = [
    '06439006-11b6-44f0-8545-c9d39035f32a', 
    '9394fb49-583f-4426-8d2f-a2b0ffb584d2', 
    '66dea7b5-0aca-11e7-82f6-3863bb44ef7c', 
    '66de85d2-0aca-11e7-82f6-3863bb44ef7c', 
    '66de25bd-0aca-11e7-82f6-3863bb44ef7c', 
    '66dc2c78-0aca-11e7-82f6-3863bb44ef7c', 
    '66dc292c-0aca-11e7-82f6-3863bb44ef7c', 
    '66dd17a5-0aca-11e7-82f6-3863bb44ef7c', 
    'bc5235a5-7f10-4a27-806e-9c25fa700959', 
    '32cc1603-a4b5-4099-b7bf-681de451b04a'
]

def fetch_and_store_static_info():
    """ Runs once when app starts to load our 10 selected stations """
    conn = get_db_connection()
    c = conn.cursor()
    c.execute("SELECT COUNT(*) FROM Station")
    count = c.fetchone()[0]
    
    # Only insert if no stations exist
    if count < len(TARGET_STATIONS):
        logging.info("Initializing static data for fixed 10 stations...")
        try:
            response = requests.get(INFO_API_URL)
            stations = response.json()["data"]["stations"]
            
            for station in stations:
                if station["station_id"] in TARGET_STATIONS:
                    c.execute(
                        "INSERT OR IGNORE INTO Station (station_id, name, lat, lon, capacity) VALUES (?, ?, ?, ?, ?)",
                        (station["station_id"], station["name"], station["lat"], station["lon"], station["capacity"])
                    )
                    
            conn.commit()
            logging.info("Successfully fetched and inserted the 10 fixed stations.")
        except Exception as e:
            logging.error(f"Failed to fetch static data: {e}")
    else:
        logging.info("Fixed stations already loaded in DB.")
    conn.close()

def job_fetch_realtime_status():
    """ 
    This function is executed every 30 seconds by the scheduler.
    It grabs current status of bikes and saves into DB. 
    """
    try:
        response = requests.get(STATUS_API_URL, timeout=10)
        json_data = response.json()
        stations = json_data["data"]["stations"]
        
        # Using our server's exact timestamp ensures we always have monotonically increasing 
        # points on the chart every 30 seconds, even if the Citi Bike API delays its update.
        actual_time = int(time.time())
        
        conn = get_db_connection()
        c = conn.cursor()
        
        inserted_count = 0
        for station in stations:
            # Only care about our 20 selected stations
            if station["station_id"] in TARGET_STATIONS:
                c.execute(
                    """
                    INSERT INTO Status (
                        station_id,
                        num_bikes_available,
                        num_docks_available,
                        is_installed,
                        is_renting,
                        is_returning,
                        num_bikes_disabled,
                        num_docks_disabled,
                        grab_time
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        station["station_id"],
                        station["num_bikes_available"],
                        station["num_docks_available"],
                        int(station.get("is_installed", 0)),
                        int(station.get("is_renting", 0)),
                        int(station.get("is_returning", 0)),
                        int(station.get("num_bikes_disabled", 0)),
                        int(station.get("num_docks_disabled", 0)),
                        actual_time,
                    )
                )
                inserted_count += 1
                
        conn.commit()
        conn.close()
        
        logging.info(f"Background task executed: fetched and saved {inserted_count} real-time records at {actual_time}")
    except Exception as e:
        logging.error(f"Error occurring during background task: {e}")
