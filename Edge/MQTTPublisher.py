import json
import time
import requests
from paho.mqtt import client as mqtt_client

if __name__ == '__main__':
    mqtt_ip = "127.0.0.1"
    mqtt_port = 1883
    topic = "CSC8112"

    # The Target URL...
    url = ("https://newcastle.urbanobservatory.ac.uk/api/v1.1/sensors/PER_AIRMON_MONITOR1135100/data/json/?starttime=20230601&endtime=20230831")

    try:
        # Request data from Newcastle Urban Observatory Platform...
        print("Fetching raw data from the Newcastle Urban Observatory...", flush=True)
        resp = requests.get(url)

        # Convert the JSON response to a dictionary format...
        raw_data_dict = resp.json()

        # Extract the PM2.5 sensor data...
        Sensor_data = raw_data_dict["sensors"][0]["data"]["PM2.5"] #Access the 0th index of 'sensors'...
        # PM2.5 data lies inside the 'PM2.5' list which is inside the 'data' list...

        print(f"Fetched {len(Sensor_data)} PM2.5 related fields...", flush=True)

        output_data = []
        for data in Sensor_data:
            sensor_timestamp = data["Timestamp"]
            sensor_value = data["Value"]
            output_data.append(str(sensor_timestamp) + "," + str(sensor_value))

        # Create a mqtt client object
        client = mqtt_client.Client(
            protocol=mqtt_client.MQTTv311,
            callback_api_version=mqtt_client.CallbackAPIVersion.VERSION2
        )
        print("MQTT client successfully created!", flush=True)

        # Callback function for MQTT connection
        def on_connect(client, userdata, flags, rc, properties=None):
            if rc == 0:
                print("Connected to MQTT OK!", flush=True)
            else:
                print(f"Failed to connect, return code: {rc}", flush=True)

        # Connect to MQTT service
        client.on_connect = on_connect
        print(f"Connecting to EMQX service (MQTT broker) at {mqtt_ip}:{mqtt_port}...", flush=True)
        client.connect(mqtt_ip, mqtt_port)

        # Start the loop...
        client.loop_start()
        time.sleep(1)  # Wait for the connection to start...

        # Send data to MQTT...
        print("Sending data...", flush=True)
        msg = json.dumps(output_data)
        result = client.publish(topic, msg)

        # Check if the data is sent...
        if result.rc == 0:
            print(f"Successfully sent {len(output_data)} readings to MQTT...", flush=True)
        else:
            print(f"Failed to publish data, return code: {result.rc}", flush=True)

        # Wait 1 second to ensure message is sent...
        time.sleep(1)

    except requests.exceptions.RequestException as e:
        print(f"Error fetching data: {e}", flush=True)
    except Exception as e:
        print(f"Error: {e}", flush=True)
    finally:
        if 'client' in locals():
            client.loop_stop()
            client.disconnect()
            print("Disconnected from MQTT broker", flush=True)
