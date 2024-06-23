import csv
from rediscluster import RedisCluster
import pandas as pd

# Environment variables for Redis Cluster and S3 configuration
REDIS_HOST = 'eventos-riesgo.zs75jb.clustercfg.usw2.cache.amazonaws.com'
REDIS_PORT =  6379

def create_redis_cluster_client():
    startup_nodes = [{"host": REDIS_HOST, "port": REDIS_PORT}]
    return RedisCluster(startup_nodes=startup_nodes, decode_responses=True, skip_full_coverage_check=True)


def upload_csv_to_redis(csv_file):
    # Connect to the Redis cluster
    r = create_redis_cluster_client()
    # Read the CSV file
    data = pd.read_csv(csv_file)

    # Iterate over each row in the CSV and upload to Redis
    for index, row in data.iterrows():
        print(row)
        estado = row['estado_evento']
        if isinstance(estado, bool):
            estado = str(estado).lower()

        event_id = row['id_evento']
        event_data = {
            'latitud_evento': row['latitud_evento'],
            'longitud_evento': row['longitud_evento'],
            'tipo_evento': row['tipo_evento'],
            'descripcion': row['descripcion'],
            'ultima_actualizacion': row['ultima_actualizacion'],
            'estado_evento': estado
        }

        # Save the event data to Redis using a hash
        r.hmset(f'event:{event_id}', event_data)
        print(f"Uploaded event ID {event_id} to Redis")

if __name__ == "__main__":
    # Define the CSV file path
    csv_file = './final3(1).csv'

    # Redis connection details


    # Upload the CSV data to Redis
    upload_csv_to_redis(csv_file)
