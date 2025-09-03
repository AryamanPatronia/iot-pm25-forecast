# Internet of Things (CSC8112) - PM2.5 Data Pipeline

**Coursework for CSC8112 - Internet of Things (Newcastle University)**  
Author: Aryaman Patronia  

---

## Project Aim
This project demonstrates the design and deployment of an **IoT data processing pipeline** using **Docker, EMQX, RabbitMQ, and Python operators**. The goal was to collect real-world IoT sensor data (air quality), preprocess it, and apply a machine learning model to predict trends.

---

## System Architecture
The pipeline consists of the following components:

1. **Data Injector (`DataInjector.py`)**  
   - Collects raw IoT data from Newcastle Urban Observatory API.  
   - Prints the raw JSON stream to console.

2. **Filtered Data Processor (`FilteredData.py`)**  
   - Extracts PM2.5 data (timestamp + value).  
   - Outputs filtered records.

3. **MQTT Publisher (`MQTTPublisher.py`)**  
   - Publishes PM2.5 readings to **EMQX broker** on topic `CSC8112`.  
   - Runs via Python MQTT client (`paho-mqtt`).

4. **Data Preprocessing Operator (`data_preprocessing_operator.py`)**  
   - Subscribes to EMQX topic `CSC8112`.  
   - Separates **valid data** and **outliers** (PM2.5 > 50).  
   - Calculates **daily averages** of PM2.5 values.  
   - Forwards averaged results to **RabbitMQ queue** `CSC8112`.  
   - Containerized with **Dockerfile** and `docker-compose`.

5. **Prediction Operator / RabbitMQ Consumer (`RabbitMQConsumer.py`)**  
   - Consumes averaged PM2.5 data from RabbitMQ.  
   - Reformats timestamps.  
   - Visualizes data trends using **Matplotlib**.  
   - Feeds data into ML predictor (`Prophet` model) to forecast **15-day PM2.5 trends**.  
   - Saves plots as `.png`.

---

## Tech Stack
- **Programming:** Python 3.8+  
- **Messaging:** EMQX (MQTT broker), RabbitMQ  
- **Containerization:** Docker, Docker Compose  
- **Visualization:** Matplotlib  
- **Machine Learning:** Prophet (via provided ML engine)  
- **Cloud Environment:** Azure Lab (Edge & Cloud VMs)  
