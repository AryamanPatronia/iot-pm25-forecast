import pika
import json
from datetime import datetime

def callback(ch, method, properties, body):
    try:
        # Decode and parse the message
        message = json.loads(body.decode('utf-8'))

        # Update the keys according to the actual message format
        timestamp = message.get('first_timestamp')
        value = message.get('average_pm25')

        if timestamp is not None and value is not None:
            # Convert the timestamp to a datetime object
            timestamp_dt = datetime.utcfromtimestamp(timestamp / 1000)  # Convert milliseconds to seconds
            formatted_timestamp = timestamp_dt.strftime('%Y-%m-%d %H:%M:%S')  # Format the datetime

            # Print the data with the reformatted timestamp
            print(f"Received PM2.5 Data: Timestamp: {formatted_timestamp}, Value: {value}")
        else:
            print(f"Received invalid message: {message}")

        # Acknowledge the message after processing
        ch.basic_ack(delivery_tag=method.delivery_tag)

    except json.JSONDecodeError:
        print("Failed to decode JSON from message payload.")
    except Exception as e:
        print(f"Error processing message: {e}")


if __name__ == '__main__':
    rabbitmq_ip = "localhost"  # RabbitMQ IP address
    rabbitmq_port = 5672      # RabbitMQ port (default 5672)
    rabbitmq_queue = "CSC8112"  # Queue name to consume from

    # Connect to RabbitMQ service with timeout
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(host=rabbitmq_ip, port=rabbitmq_port, socket_timeout=60)
    )
    channel = connection.channel()

    # Declare the queue (ensures the queue exists before consuming messages)
    channel.queue_declare(queue=rabbitmq_queue, durable=True)

    # Set QoS to allow more than one message at a time (prefetch count)
    channel.basic_qos(prefetch_count=10)

    # Start consuming messages from the queue
    channel.basic_consume(queue=rabbitmq_queue, on_message_callback=callback, auto_ack=False)

    print(f"Waiting for messages from {rabbitmq_queue}. To exit press CTRL+C")

    try:
        # Start consuming messages
        channel.start_consuming()
    except KeyboardInterrupt:
        print("\nDisconnecting from RabbitMQ...")
        channel.stop_consuming()
        connection.close()
