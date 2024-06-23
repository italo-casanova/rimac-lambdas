import json
import os
import boto3
from rediscluster import RedisCluster

# Environment variables for Redis Cluster and S3 configuration
REDIS_HOST = os.getenv('REDIS_HOST', 'eventos-riesgo.zs75jb.clustercfg.usw2.cache.amazonaws.com')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD', None)
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'to-be-sent')

# Define risk categories and their corresponding search radii
RISK_CATEGORIES = [
    (0, 10, 10),
    (10, 40, 20),
    (50, 70, 40),
    (80, 100, 70)
]

# Initialize the S3 client
s3_client = boto3.client('s3')

def create_redis_cluster_client():
    startup_nodes = [{"host": REDIS_HOST, "port": REDIS_PORT}]
    return RedisCluster(startup_nodes=startup_nodes,
                        decode_responses=True,
                        password=REDIS_PASSWORD,
                        skip_full_coverage_check=True
                        )

def create_event_json(event_id, user_id, distance, event_data):
    return {
        "id_evento": event_id,
        "id_usuario": user_id,
        "distancia_al_evento": distance,
        "tipo_evento": event_data.get('tipo_evento', ''),
        "descripcion_evento": event_data.get('descripcion', ''),
        "ubicacion": None,
        "nivel_riesgo": None,
        "hora_evento": event_data.get('hora', '')
    }

def store_event_in_s3(event_json):
    object_key = f"event_{event_json_test['id_evento']}.json"
    s3_client.put_object(
        Bucket=S3_BUCKET_NAME,
        Key=object_key,
        Body=json.dumps(event_json),
        ContentType='application/json'
    )

def lambda_handler(event, context):
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

    nearest_events = {}
    for (min_risk, max_risk, radius) in RISK_CATEGORIES:
        nearest_event = None
        nearest_distance = float('inf')

        try:
            print(f"Searching within {radius} km for risk level {min_risk}-{max_risk}")
            results = redis_client.georadius('events', current_location[1], current_location[0], radius, 'km', withdist=True, count=1)
            for result in results:
                event_id = result[0]
                distance = float(result[1])
                event_data = redis_client.hgetall(f'event:{event_id}')
                nivel_riesgo = int(event_data.get('nivel_riesgo', 0))

                if min_risk <= nivel_riesgo < max_risk and distance < nearest_distance:
                    nearest_distance = distance
                    nearest_event = event_data
        except Exception as e:
            return {
                'statusCode': 500,
                'body': json.dumps(f'Error during GEORADIUS command: {str(e)}')
            }

        if nearest_event:
            event_json = create_event_json(event_id, user_id, nearest_distance, nearest_event)
            store_event_in_s3(event_json)
            nearest_events[f'{min_risk}-{max_risk}'] = event_json

        return {
            'statusCode': 200,
            'body': json.dumps(nearest_events)
        }

# For local testing
if __name__ == "__main__":
    test_event = {
        "latitud": -12.046374,
        "altitud": -77.042793,
        "id_usuario": "user123"
    }
    print(lambda_handler(test_event, {}))
