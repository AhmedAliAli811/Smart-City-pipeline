import os
import time
import uuid
from confluent_kafka import SerializingProducer
import simplejson as json
from datetime import datetime , timedelta
import random

CAIRO_COORDINATES = {"latitude":30.06263 , "longitude": 31.24967}
ASWAN_COORDINATES = {"latitude":24.09082 , "longitude": 32.89942}

# Movement increment
LATITUDE_INCREMENT = (ASWAN_COORDINATES["latitude"] - CAIRO_COORDINATES["latitude"]) / 100  
LONGITUDE_INCREMENT = (ASWAN_COORDINATES["longitude"] - CAIRO_COORDINATES["longitude"]) / 100  

# Environment Variables 

KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS' , 'localhost:9092')

VEHICLE_TOPIC = os.getenv('VEHICLE_TOPIC' , 'vehicle_data')
GPS_TOPIC  = os.getenv('GPS_TOPIC' , 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC' , 'traffic_data')
WEATHER_TOPIC = os.getenv('WEATHER_TOPIC' , 'weather_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC' , 'emergency_data')
random.seed(42)
start_time = datetime.now()
start_location = CAIRO_COORDINATES.copy()
def get_next_time():
    global start_time

    start_time += timedelta(seconds=random.randint(30 , 60))
    return start_time

def simulate_vehicle_movement(device_id):
    global start_location


    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT

    start_location['latitude'] += random.uniform(-0.0005 , 0.0005)
    start_location['longitude'] += random.uniform(-0.0005 , 0.0005)

    return start_location

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement(device_id)

    return {
        'id':uuid.uuid4() , 
        'device_id':device_id ,
        'timestamp':get_next_time().isoformat(),
        'location':(location['latitude'] , location['longitude']),
        "speed": random.uniform(0 , 180),
        'direction':'South-West',
        'make': 'porsche',
        'model':'911-GT3-RS',
        'year': 2026,
        'fuelType': 'GAS'
    }

def generate_traffic_camera_data(device_id , timestamp ,location ,  camera_id):

    return {
        'id':uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        'camera_id': camera_id,
        'location': location,
        'snapshot': 'Base64EncodedString'
    }

def generate_gps_data(device_id , timestamp , vehicle_type = 'private'):

    return {
        'id':uuid.uuid4(),
        'device_id': device_id,
        'timestamp': timestamp,
        "speed": random.uniform(0 , 180),
        'direction':'South-West',
        'vehicle_type': vehicle_type
    }
def generate_weather_data(device_id , timestamp, location):
    temperature  = random.uniform(20 , 42)
    ## Conditions = ['Sunny', 'Cloudy', 'Hot' , 'Partly Cloudy']
    weatherCondition = None
    if 20 <= temperature < 25:
        weatherCondition = 'Cloudy'
    elif 25 <= temperature < 30 :
        weatherCondition = 'Partly Cloudy'
    elif 30 <= temperature < 35:
        weatherCondition = 'Sunny'
    else:
        weatherCondition = 'Hot'
        
    return {
            'id':uuid.uuid4(),
            'device_id': device_id,
            'timestamp': timestamp,
            'location': location,
            'temperature': temperature,
            'weatherCondition': weatherCondition,
            'feels_like': temperature + random.uniform(0, 8),
            "humidity": random.randint(20, 95),
            "pressure": random.randint(1000, 1025),
            'windSpeed': random.uniform(0 , 100),
            "visibility": random.uniform(1, 10),
            "uv_index": random.randint(0, 11),
            "is_day": random.choice([True, False])
    }

def generate_emergency_incident_data(device_id , timestamp , location):
    return {
            'id':uuid.uuid4(),
            'device_id': device_id,
            'timestamp': timestamp,
            'location': location,
            'incidentID': uuid.uuid4(),
            'type': random.choice(['Accident' , 'Fire' , 'Medical' , 'Police' , 'None']),
            'status': random.choice(['Active' , 'Resolved'])
        
        }
def json_serializer(obj):
    if isinstance(obj , uuid.UUID):
        return str(obj)

    raise TypeError(f'Object of type {obj.__class__.__name__} is not json serializable')

def delivery_report(err , msg):
    if err is not None:
        print(f'Message delivery Failed: {err}')
    else:
        print(f'Message delivered Successfully to {msg.topic()} [{msg.partition()}]')


def produce_to_kafka(producer:SerializingProducer , topic:str , data:dict):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data , default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()


def simulate_journey(producer , device_id):

    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id , vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id , vehicle_data['timestamp'] , vehicle_data['location'] , 'Canon-123')
        weather_data = generate_weather_data(device_id , vehicle_data['timestamp'] , vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id , vehicle_data['timestamp'] , vehicle_data['location'])


        produce_to_kafka(producer , VEHICLE_TOPIC , vehicle_data)
        produce_to_kafka(producer , GPS_TOPIC , gps_data)
        produce_to_kafka(producer , TRAFFIC_TOPIC , traffic_camera_data)
        produce_to_kafka(producer , WEATHER_TOPIC , weather_data)
        produce_to_kafka(producer , EMERGENCY_TOPIC , emergency_incident_data)

        if vehicle_data['location'][0] <= ASWAN_COORDINATES['latitude'] and vehicle_data['location'][1] >= ASWAN_COORDINATES['longitude']:
            print(f'Vehicle Reached Aswan, Simulation Ending...') 
            break
        time.sleep(5)

if __name__ == "__main__":
    producer_configs = {
        'bootstrap.servers':KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda e: print(f"Kafka error : {e}")
    }

    producer = SerializingProducer(producer_configs)

    try:
        simulate_journey(producer , "ahmed_porsche_911")

    except KeyboardInterrupt:
        print("Simulation ended by user")

    except Exception as e:
        print(f'Unexpected Error occurred  : {e}')
        