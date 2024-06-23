import json
import os
import boto3
from rediscluster import RedisCluster

# Environment variables for Redis Cluster and S3 configuration
REDIS_HOST = 'eventos-riesgo.zs75jb.clustercfg.usw2.cache.amazonaws.com'
REDIS_PORT =  6379
S3_BUCKET_NAME = 'zona-aterrizaje-ubicaciones'

# Define risk categories and their corresponding search radii
RADIUS = 20
# Initialize the S3 client
s3_client = boto3.client('s3')

def get_relevance(distance):
    SLOPE = -1 * (1 / 20)
    INTERCEP =  1

    return SLOPE * distance + INTERCEP

def create_redis_cluster_client():
    startup_nodes = [{"host": REDIS_HOST, "port": REDIS_PORT}]
    return RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)

def create_event_json(event_id, user_id, distance, event_data):
    return {
        "id_evento": event_id,
        "id_usuario": user_id,
        "distancia_al_evento": distance,
        "tipo_evento": event_data.get('tipo_evento', ''),
        "descripcion_evento": event_data.get('descripcion', ''),
        "ubicacion": event_data.get('ubicacion', ''),
        "nivel_riesgo": event_data.get('nivel_riesgo', ''),
        "hora_evento": event_data.get('hora', '')
    }

def store_event_in_s3(event_json):
    object_key = f"event_test_1{event_json['id_evento']}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=object_key,
        Body=json.dumps(event_json),
        ContentType='application/json'
    )

def lambda_handler(event, context):
    print(event)
    try:
        redis_client = create_redis_cluster_client()
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Failed to connect to Redis Cluster: {str(e)}')
        }

    try:
        current_lat = float(event.get('latitud'))
        current_lon = float(event.get('altitud'))
        user_id = event.get('id_usuario')
        current_location = (current_lat, current_lon)
    except (TypeError, ValueError) as e:
        return {
            'statusCode': 400,
            'body': json.dumps(f'Invalid input data: {str(e)}')
        }

    nearest_events = []
    try:
        print(f"Searching within {RADIUS} km")
        results = redis_client.georadius('events_locations', current_location[1], current_location[0], RADIUS, 'km', withdist=True, count=1)
        print(redis_client.georadius('events_locations', -79.935242, 40.73061, 10000, unit='km', withcoord=True))
        print("results:", results)
        distance = 0
        for result in results:
            event_id = result[0]
            distance = float(result[1])
            event_data = redis_client.hgetall(f'event:{event_id}')
            nivel_riesgo = int(event_data.get('nivel_riesgo', 10))
            descripcion = int(event_data.get('descripcion'))
            tipo_evento = int(event_data.get('tipo_evento'))
            # create a dict with the event data

            print("NIVEL", nivel_riesgo)
            nearest_events.append(event_data)
            print(distance)

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error during GEORADIUS command: {str(e)}')
        }

    if nearest_events:
        for i  in range(5):
            event_json = create_event_json(event_id , user_id, nearest_distance, nearest_event)
            store_event_in_s3(event_json)

            # nearest_events[] = event_json

        return {
            'statusCode': 200,
            'body': json.dumps(nearest_events)
        }

    return {
            'statusCode': 200,
            'body': "no funciona"
        }
