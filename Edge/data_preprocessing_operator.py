import json
import time
import paho.mqtt.client as mqtt_client
import datetime
import pika

# MQTT SETUP...
mqtt_ip = "127.0.0.1"
mqtt_port = 1883
topic = "CSC8112"

# RABBITMQ SETUP...
rabbitmq_host = "localhost"
rabbitmq_queue = "CSC8112"

# Print out the received data...(callback function)...
def on_message(client, userdata, msg):
    try:
        # Parse the JSON message...
        data = json.loads(msg.payload.decode())
        print("Received data:", flush=True)

        # Lists to store valid data and outliers separately...
        valid_data = []
        outliers = []

        for entry in data:
            timestamp, value = entry.split(",")
            value = float(value)  # Convert the value to float for comparison...

            # Check for outliers (value greater than 50)...
            if value > 50:
                outliers.append((timestamp, value)) #Append with Outliers list...
            else:
                valid_data.append((timestamp, value)) #Append with Valid data list...

        # Print outliers...
        print("\nOutliers detected:")
        for timestamp, value in outliers:
            print(f"Timestamp: {timestamp}, PM2.5 Value: {value}")

        # Print valid data (no outliers)...
        print("\nValid data:")
        for timestamp, value in valid_data:
            print(f"Timestamp: {timestamp}, PM2.5 Value: {value}")

        # Calculate daily average PM2.5 values...
        daily_data = calculate_daily_average(valid_data)

        # Print the daily averages...
        print("\nAveraged PM2.5 data per day:")
        for timestamp, avg_value in daily_data:
            print(f"Date: {timestamp}, Averaged PM2.5 Value: {avg_value}")

        # Send the daily averaged data to RabbitMQ service on AzureLab(Cloud)...
        send_to_rabbitmq(daily_data)

    except json.JSONDecodeError:
        print("Error decoding JSON message :(", flush=True)
    except Exception as e:
        print(f"Error processing message: {e}", flush=True)

# Function to calculate daily averages...
def calculate_daily_average(valid_data):
    daily_totals = {}
    daily_counts = {}

    for timestamp, value in valid_data:
        # Convert timestamp to a datetime object if it's a Unix timestamp...
        if timestamp.isdigit():  # Check if it's a Unix timestamp...
            timestamp_dt = datetime.datetime.fromtimestamp(int(timestamp) / 1000)  # Convert from milliseconds to seconds...
        else:
            timestamp_dt = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")  # For formatted timestamps...
        # Use the date as the key (ignoring time for daily aggregation)
        date_key = timestamp_dt.date()

        if date_key not in daily_totals:
            daily_totals[date_key] = 0
            daily_counts[date_key] = 0

        daily_totals[date_key] += value
        daily_counts[date_key] += 1

    # Calculate the average for each day...
    daily_averages = []
    for date_key in daily_totals:
        avg_value = daily_totals[date_key] / daily_counts[date_key]
        # Use the first timestamp of the day as the new timestamp for the average...
        first_timestamp_of_day = datetime.datetime.combine(date_key, datetime.time(0, 0)).strftime("%Y-%m-%d %H:%M:%S")
        daily_averages.append((first_timestamp_of_day, avg_value))

    return daily_averages

# Function to send the data to RabbitMQ...
def send_to_rabbitmq(daily_data):
    try:
        # Establish a connection with RabbitMQ...
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()

        # Declare a queue...
        channel.queue_declare(queue=rabbitmq_queue,durable=True) #Setting 'durable=True'...

        # Send each daily data entry to RabbitMQ...
        for timestamp, avg_value in daily_data:
            message = json.dumps({"timestamp": timestamp, "pm25_value": avg_value})
            channel.basic_publish(exchange='',
                                  routing_key=rabbitmq_queue,
                                  body=message)
            print(f"Sent to RabbitMQ: {message}")

        # Close the connection...
        connection.close()
    except Exception as e:
        print(f"Error sending data to RabbitMQ: {e}", flush=True)

# Create a new MQTT client...
client = mqtt_client.Client()

# Callback for when the client connects...
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print("Connected to MQTT broker", flush=True)
        # Subscribe to the topic 'CSC8112' after connecting...
        client.subscribe(topic)
    else:
        print(f"Failed to connect, return code: {rc}", flush=True)

# Set up the client...
client.on_connect = on_connect
client.on_message = on_message

# Connect to the MQTT broker...
print(f"Connecting to MQTT broker at {mqtt_ip}:{mqtt_port}...", flush=True)
client.connect(mqtt_ip, mqtt_port)

# Start the loop to process messages...
client.loop_start()

# Run the consumer...
try:
    print("Waiting for messages...", flush=True)
    while True:
        time.sleep(1)  # Keep the program running so it processes messages...
except KeyboardInterrupt:
    print("Consumer stopped...", flush=True)
finally:
    # Clean up MQTT connection...
    client.loop_stop()
    client.disconnect()
    print("Disconnected from MQTT broker...", flush=True)
