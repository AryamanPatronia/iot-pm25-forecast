import requests

if __name__ == '__main__':
    # Using the URL to get the raw data from Newcastle Urban Observatory API...
    url = "https://newcastle.urbanobservatory.ac.uk/api/v1.1/sensors/PER_AIRMON_MONITOR1135100" \
          "/data/json/?starttime=20230601&endtime=20230831"

    # Requesting data from Urban Observatory Platform
    response = requests.get(url)
    RawData = response.json()

    sensors = RawData.get("sensors")
    data = sensors[0].get("data")  # The 0th index...
    FilteredData = data.get("PM2.5")  # Getting the PM2.5 Data...
    # Storing the PM2.5 data in a list...
    PM25DATA = []

    # For printing out the timestamp and value...
    for entry in FilteredData:
        timestamp = entry.get("Timestamp")
        value = entry.get("Value")
        PM25DATA.append({'timestamp': timestamp, 'value': value})

    # Printing the filtered PM2.5 Data...
    print("Filtered PM2.5 data:", PM25DATA)