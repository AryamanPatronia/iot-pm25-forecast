import os
import sys
sys.stderr = open(os.devnull, 'w')  # Suppress GTK/AT-SPI warnings

import matplotlib
matplotlib.use('Agg')  # Non-interactive backend for matplotlib
import json
import pika
import datetime
import pandas as pd
from   Cloud.ml_engine import MLPredictor
import threading
import time
import matplotlib.pyplot as plt


#THIS IS OUR PREDICTION OPERATOR / RABBITMQ CONSUMER...

# RabbitMQ SETUP...
rabbitmq_host = "192.168.0.100"
rabbitmq_queue = "CSC8112"

# Storage for received data...
pm25_data = []
data_collection_done = threading.Event()  # Event to signal when the data collection is complete...

# Callback function for RabbitMQ consumer...
def on_message(ch, method, properties, body):
    global pm25_data
    try:
        # Parse the JSON message...
        data = json.loads(body.decode())
        timestamp = data['timestamp']
        pm25_value = data['pm25_value']

        # Convert timestamp to datetime...
        timestamp_dt = datetime.datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S")
        reformatted_timestamp = timestamp_dt.strftime("%Y-%m-%d %H:%M:%S")  # Reformatted to include time...

        # Append to the dataset...
        pm25_data.append({'Timestamp': reformatted_timestamp, 'PM2.5': pm25_value})

        # Print reformatted timestamp and PM2.5 value to console...
        print(f"Data received: Reformatted Timestamp={reformatted_timestamp}, PM2.5={pm25_value}", flush=True)

        # Acknowledge the message...
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except Exception as e:
        print(f"Error processing message: {e}", flush=True)



def visualize_daily_averages(data):
    plt.figure(figsize=(10, 5), dpi=200)
    plt.plot(data['Timestamp'], data['Value'], color="#FF3B1D", marker='o', linestyle="-")
    plt.title("Averaged PM2.5 Data")
    plt.xlabel("Dates")
    plt.ylabel("PM2.5 Values")

    # Set all dates on x-axis with the font size '7'...
    # We are just putting the date to keep it clean...
    plt.xticks(data['Timestamp'], rotation=90, fontsize=6)
    plt.tight_layout()
    plt.savefig("daily_pm25.png")
    plt.show()

# Function to predict PM2.5 trends...
def predict_and_visualize(data):
    predictor = MLPredictor(data)  # Instantiate MLPredictor as an object...
    predictor.train()  #Using the train() method of MLPredictor...
    forecast = predictor.predict()  #Using the predict() method of MLPredictor...
    fig = predictor.plot_result(forecast)  #Using the plot_result() method of MLPredictor...
    fig.savefig("pm25_prediction.png")
    fig.show()


# Function to process and analyze the data...
def process_data():
    global pm25_data
    if not pm25_data:
        print("No data received to process :(")
        return

    # Create a DataFrame from collected data...
    df = pd.DataFrame(pm25_data)
    df['Timestamp'] = pd.to_datetime(df['Timestamp'])

    # Calculate daily averages...
    daily_avg = df.groupby(df['Timestamp'].dt.date)['PM2.5'].mean().reset_index()
    daily_avg.rename(columns={"Timestamp": "Timestamp", "PM2.5": "Value"}, inplace=True)

    print("Daily Averaged PM2.5 Data:")
    print(daily_avg)

    # Visualize the daily averages...
    visualize_daily_averages(daily_avg)

    # Feed averaged data into the ML predictor...
    predict_and_visualize(daily_avg)

# Function to stop the RabbitMQConsumer...
def stop_consumer(channel, connection):
    def stop():
        if channel.is_open:
            channel.stop_consuming()
        if connection.is_open:
            connection.close()
        data_collection_done.set()  # Signal that data collection is complete...
    connection.add_callback_threadsafe(stop)

# Function to start the prediction operator...
def start_consumer(timeout=30):
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_host))
        channel = connection.channel()

        # Declare the queue...
        channel.queue_declare(queue=rabbitmq_queue, durable=True)

        # Set up consumer...
        channel.basic_consume(queue=rabbitmq_queue, on_message_callback=on_message)

        # Start a thread to stop the consumer after a timeout...
        threading.Thread(target=lambda: (time.sleep(timeout), stop_consumer(channel, connection)), daemon=True).start()

        print(f"Waiting for messages from RabbitMQ queue: {rabbitmq_queue} for {timeout} seconds...", flush=True)
        channel.start_consuming()

    except Exception as e:
        print(f"Error connecting to RabbitMQ: {e}", flush=True)

# Main function to run consumer and process the incoming data...
if __name__ == "__main__":
    try:
        print("Starting RabbitMQ consumer...", flush=True)
        start_consumer(timeout=30)  # Collect data for 30 seconds...
        data_collection_done.wait()  # Wait until data collection is done...

        print("\nProcessing data...")
        process_data()

    except KeyboardInterrupt:
        print("\nKeyboard interrupt detected. Stopping data collection...")
