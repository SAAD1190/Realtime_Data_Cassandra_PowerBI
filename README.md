# Smart City Data Pipeline with Cassandra and Power BI

This project showcases an **end-to-end real-time data pipeline** for smart city applications. It integrates **Apache Kafka**, **Apache Spark**, **Cassandra**, and **Power BI** to handle, process, and visualize real-time sensor data.

## Project Overview

The pipeline simulates IoT sensor data, streams it using Kafka, processes it with Spark, and stores it in Cassandra for high availability and scalability. The processed data is then visualized in Power BI dashboards, enabling actionable insights.

## Features

- **Real-time data simulation**: Generate sensor data using a Python script.
- **Message brokering with Kafka**: Manage data streams efficiently.
- **Real-time processing with Spark**: Perform computations like calculating averages.
- **NoSQL database (Cassandra)**: Store raw and processed data with high availability and scalability.
- **Visualization with Power BI**: Create real-time dashboards for interactive monitoring.

## Technologies Used

- **Apache Kafka**: For message brokering and streaming.
- **Apache Spark**: For distributed real-time data processing.
- **Apache Cassandra**: For scalable and fault-tolerant data storage.
- **Power BI**: For live data visualization.
- **Python**: For simulating sensor data and connecting components.

## System Architecture

1. **Simulated Sensor Data**: A Python script generates real-time sensor data (e.g., speed, fuel consumption, GPS coordinates).
2. **Kafka**: Streams the generated data to a Kafka topic.
3. **Spark**: Processes data from Kafka in real-time and computes metrics like average speed.
4. **Cassandra**: Stores the processed and raw data for high availability.
5. **Power BI**: Visualizes real-time data through interactive dashboards.
