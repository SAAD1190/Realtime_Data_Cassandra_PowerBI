import os
import random
import time
import uuid

from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime, timedelta

# Coordinates for Kenitra and Rabat
CityB_COORDINATES = {"latitude": 34.2610, "longitude": -6.5802}
CityA_COORDINATES = {"latitude": 34.0209, "longitude": -6.8416}

# Calculate movement increments
LATITUDE_INCREMENT = (CityA_COORDINATES['latitude'] - CityB_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (CityA_COORDINATES['longitude'] - CityB_COORDINATES['longitude']) / 100

# Environment Variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC', 'vehicle_data')

random.seed(42)
start_time = datetime.now()
start_location = CityB_COORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30, 60))  # update frequency
    return start_time


def simulate_vehicle_movement():
    global start_location
    # move towards Rabat
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    # add some randomness to simulate actual road travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)

    return start_location


def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 80),  # Updated speed range
        'carModel': 'Toyota',         
        'fuelType': 'Gasoline',            # Changed fuel type
        'fuelConsumption': random.uniform(4, 9),  # Fuel consumption between 5 and 10 L/100km
        'tirePressure': round(random.uniform(2.4, 2.5), 2),  # Tire pressure between 2.4 and 2.5
        'alAlloyConcentration': random.uniform(0.01, 0.05),   # Random metal concentration in oil
        'oilTemperature': random.uniform(85, 95)           # Oil temperature between 85 and 95 degrees
    }


def json_serializer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)
    raise TypeError(f'Object of type {obj.__class__.__name__} is not JSON serializable')


def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')


def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )

    producer.flush()


def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)

        if (vehicle_data['location'][0] <= RABAT_COORDINATES['latitude']
                and vehicle_data['location'][1] <= RABAT_COORDINATES['longitude']):
            print('Vehicle has reached Rabat. Simulation ending...')
            break

        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)

        time.sleep(5)  # simulate 5 seconds of travel time


if __name__ == "__main__":
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer, 'Vehicle-KenitraRabat-001')

    except KeyboardInterrupt:
        print('Simulation ended by the user')
    except Exception as e:
        print(f'Unexpected Error occurred: {e}')
