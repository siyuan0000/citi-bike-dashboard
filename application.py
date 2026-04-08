import os
import requests
from flask import Flask, render_template, jsonify

# AWS Elastic Beanstalk looks for an 'application' callable by default.
application = Flask(__name__)

# Constants
GBFS_STATION_INFO_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_information.json"
GBFS_STATION_STATUS_URL = "https://gbfs.lyft.com/gbfs/2.3/bkn/en/station_status.json"

@application.route('/')
def index():
    """Real-time data dashboard using the GBFS API"""
    return render_template('index.html')

@application.route('/history')
def history():
    """Historical data distribution using Spark (pre-computed via S3 in Lab 5)"""
    return render_template('history.html')

@application.route('/api/stations')
def api_stations():
    """API endpoint to fetch and merge station info and status"""
    try:
        # Fetch station information (static like coordinates, name)
        info_resp = requests.get(GBFS_STATION_INFO_URL)
        info_data = info_resp.json().get('data', {}).get('stations', [])
        
        # Fetch station status (dynamic like available bikes)
        status_resp = requests.get(GBFS_STATION_STATUS_URL)
        status_data = status_resp.json().get('data', {}).get('stations', [])

        # Create a dict mapping station_id to static info for quick lookup
        stations_info = {station['station_id']: station for station in info_data}

        # Merge dynamic status with static info
        merged_stations = []
        for status in status_data:
            st_id = status['station_id']
            if st_id in stations_info:
                merged = {**stations_info[st_id], **status}
                merged_stations.append(merged)

        return jsonify({'status': 'success', 'data': merged_stations})
    except Exception as e:
        return jsonify({'status': 'error', 'message': str(e)}), 500

@application.route('/api/history-data')
def api_history_data():
    """Mock API endpoint for fetching Spark-computed historical data from S3"""
    # TODO: In Lab 5, integrate boto3 to fetch pre-computed Spark results from S3
    return jsonify({
        'status': 'success', 
        'data': {'message': 'This will serve S3 data computed by Spark (Lab 5)'}
    })

if __name__ == '__main__':
    # Run the application locally
    application.run(debug=True, port=5000)
