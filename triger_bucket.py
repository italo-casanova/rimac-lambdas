import json
import boto3

lambda_client = boto3.client('lambda')
s3_client = boto3.client('s3')

def load_from_s3(bucket_name, s3_key):
    try:
        s3_object = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
        body = s3_object['Body']
        json_string = body.read().decode('utf-8')
        print("JSON string retrieved from S3:", json_string)  # Added logging
        return json.loads(json_string)
    except Exception as err:
        print("Error loading from S3:", err)  # Improved error logging
        return None

def lambda_handler(event, context):
    # TODO implement
    print("Event:", event)
    bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
    print("Bucket name:", bucket_name)
    s3_key = event["Records"][0]["s3"]["object"]["key"]
    print("Key:", s3_key)

    calificacion_evento = load_from_s3(bucket_name, s3_key)
    if calificacion_evento is None:
        print("Failed to load JSON from S3")
        return {
            'statusCode': 500,
            'body': json.dumps('Failed to load JSON from S3')
        }

    print("Loaded JSON from S3:", calificacion_evento)
    invoke_response = lambda_client.invoke(FunctionName="enviar-eventos-relevantes",
                                            InvocationType='Event',
                                            Payload=json.dumps(calificacion_evento))  # Use json.dumps
    print('Lambda invoke response:', invoke_response)
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
