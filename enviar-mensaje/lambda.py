import json
import boto3
import logging
from datetime import datetime, timedelta
from boto3.dynamodb.conditions import Key, Attr
logger = logging.getLogger(__name__)


sns_client = boto3.client('sns')
s3_client = boto3.client('s3')
bedrock_runtime = boto3.client('bedrock-runtime')
dynamodb = boto3.resource('dynamodb')
tabla_registro_mensajes = dynamodb.Table('registro-mensajes')

def enviar_mensaje(mensaje, numero, deduplicationId, id_usuario, id_evento):
	# enviar mensaje	
	response = sns_client.publish(
		Message=mensaje,
		PhoneNumber=numero,
		MessageGroupId='test-group',
		MessageDeduplicationId='hishsassdfdfdf',
		
		MessageAttributes={
			'AWS.SNS.SMS.SMSType': {
				'DataType': 'String',
				'StringValue': 'Promotional'
				
			}
		}
	)

	print('se ha enviado el mensaje')
	
	# cargar mensaje
		
	#add to registro-mensajes
	utc_datetime = datetime.now().isoformat()
	
	#inserting values into table 
	respuesta_item_tabla = tabla_registro_mensajes.put_item( 
	Item={
		'tiempo_envio': str(utc_datetime),
		'id_evento': id_evento,
		'id_usuario': id_usuario
	}
	)
	print('se ha cargado con exito el item ', respuesta_item_tabla)
	
	return response

def load_from_s3(bucket_name, s3_key):
	try:
		
		object = s3_client.get_object(Bucket=bucket_name, Key=s3_key)
		body = object['Body']
		json_string = body.read().decode('utf-8')
		return json.loads(json_string)
	except Exception as err:
		logger.error(err)
		return None
		
def generar_mensaje(bedrock_runtime, model_id, system_prompt, messages, max_tokens):
	"""
	se genera unar respuesta usando un modelo de bedrock
	"""
	body=json.dumps(
		{
			"anthropic_version": "bedrock-2023-05-31",
			"max_tokens": max_tokens,
			"system": system_prompt,
			"messages": messages
		}  
	)  

	
	response = bedrock_runtime.invoke_model(body=body, modelId=model_id)
	response_body = json.loads(response.get('body').read())
   
	return response_body

def construir_mensaje(ubicacion, descripcion_evento, distancia, tipo_ubicacion, tipo_evento):
	model_id = 'anthropic.claude-3-sonnet-20240229-v1:0'
	system_prompt ="""Eres un asistente de Rimac Seguros en Perú que da un breve resumen y recomendaciones a tomar en base a un evento de riesgo
		que se ha detectado en una ubicación cercana  a algunos de los tipos de ubicaciones que frecuenta el usuario. Este puede estar conduciendo,
		en su casa o trabajo. Le importa el tráfico, la seguridad de él y de su familia, y la seguridad de su hogar. 
		Si ocurre un sismo recomiéndale estar atento a las réplicas.
		La descripción del evento es un texto corto que describe el evento de riesgo detectado y la ubicación.
		Para las ubicaciones de tipo familiar, es importante mencionar que el usuario tiene un familiar en esa ubicación y repetir la ubicación para que el usuario pueda identificarla.
		Siempre incluye la distancia, como en "se ha detectado un [evento] a 100 metros de".
		Intenta no dejar saltos de linea donde no es necesario e intenta no incluir la palabra Recomendaciones en la generación. Presenta las recomendaciones directamente"""
	max_tokens = 100
		# Prompt con usuario y asistente

	prompt_usuario = f"Se ha detectado un evento de riesgo en {ubicacion} a {distancia} km de tu ubicación. {descripcion_evento}. ¿Qué debo hacer?"

	# tipo ubicacion = actual, se trata de una ubicacion actual
	# tipo ubicacion = frecuente, se trata de una ubicacion frecuentemente visitada
	# tipo ubicacion = familiar, se trata de una ubicacion que frecuenta un familiar
	if tipo_ubicacion == 'actual':
		prompt_usuario = f"Se ha detectado un evento de riesgo de tipo {tipo_evento} a {distancia} m de tu ubicación. Descripcion: {descripcion_evento}. Dame un informe y una breve recomendación."
	elif tipo_ubicacion == 'frecuente':
		prompt_usuario = f"Se ha detectado un evento de riesgo de tipo {tipo_evento} a {distancia} m de un lugar que sueles frecuentar. Descripcion: {descripcion_evento}. Dame un informe y una breve recomendación."
	elif tipo_ubicacion == 'familiar':
		prompt_usuario = f"Se ha detectado un evento de riesgo de tipo {tipo_evento} en ubicacion:{ubicacion} a {distancia} m de un lugar donde suele estar un familiar tuyo. Descripcion: {descripcion_evento}. Dame un informe y una breve recomendación."

	user_message =  {"role": "user", "content": prompt_usuario}
	assistant_message =  {"role": "assistant", "content": 'Rimac Informa:'}
	messages = [user_message, assistant_message]
	response = generar_mensaje(bedrock_runtime, model_id,system_prompt, messages, max_tokens)
	print("User turn and prefilled assistant response.")
	# print(json.dumps(response, indent=4))

	completion = 'Rimac Informa:' + response["content"][0]["text"] 
	print(f"Se ha completado el prompt con el siguiente texto: {completion}")
	return completion

def lambda_handler(event, context):

	number = '+51995005072'
	deduplicationId = 'sadfasfaljjljjljl'
	mensaje = 'hihihihih Ever!'

	bucket_name = event["Records"][0]["s3"]["bucket"]["name"]
	s3_key = event["Records"][0]["s3"]["object"]["key"]
	calificacion_eventos = load_from_s3(bucket_name, s3_key)

	id_usuario = calificacion_eventos['id_usuario']
	eventos = calificacion_eventos['eventos']

	print(calificacion_eventos)

	try:

		
		delta_mismo_evento = 20
		evento_a_enviar = {}
		for evento in eventos:
			id_evento = evento['id_evento']
			tipo_evento = evento['tipo_evento']
			
			
			utc_datetime = datetime.now().isoformat()
			logger.info(utc_datetime)
			delta_mismo_evento = 20 # minutos
			# excepcion para eventos de alto riesgo
			if tipo_evento == 'sismo':
				delta_mismo_evento = 10

			lower_bound = datetime.now() - timedelta(minutes=delta_mismo_evento)
			lower_bound = lower_bound.isoformat()
			print(lower_bound)
			print(utc_datetime)
			response = tabla_registro_mensajes.scan(
				FilterExpression=Attr('tiempo_envio').between(str(lower_bound),str(utc_datetime)) \
					& Attr('id_usuario').eq(id_usuario)\
					& Attr('id_evento').eq(id_evento)
			)
			items = response['Items']
			if items:
				print('ya se ha enviado este mensaje en los ultimos 20 minutos')
				continue
			else:
				# validar que no se haya enviado algun mensaje para no atosigar al usuario
				delta_cualquier_evento = 3
				# verificar si se ha enviado un mensaje en los ultimos 3 minutos
				lower_bound = datetime.now() - timedelta(minutes=delta_cualquier_evento)
				lower_bound = lower_bound.isoformat()
				response = tabla_registro_mensajes.scan(
					FilterExpression=Attr('tiempo_envio').between(str(lower_bound),str(utc_datetime)) \
					& Attr('id_usuario').eq(id_usuario)
				)
				items = response['Items']
				if items:
					print('ya se ha enviado algun mensaje estos ultimos 3 minutos')
					continue
				evento_a_enviar = evento
				break

		print(items)
		print('try sending')
		if evento_a_enviar:
			construir_mensaje(evento_a_enviar['ubicacion'], evento_a_enviar['descripcion'], evento_a_enviar['distancia'], evento_a_enviar['tipo_ubicacion'], evento_a_enviar['tipo_evento'])
			enviar_mensaje(mensaje, number, deduplicationId, id_usuario, id_evento)
			print('sent y aniadido to registro-mensajes')
		
	except Exception as e:
		# handle error and move to new s3																																																				
		logging.info('message not sent due to error. Sending to retry bucket')

if __name__ == '__main__':
	with open('test.json', 'r') as f:
		event = json.load(f)
	#lambda_handler(event, None)
	#print(construir_mensaje('Jardin de niños de mi hija', 'sismo', 200, 'familiar'))
	#construir_mensaje(None,'Dirección: JR. HUASCARAN  355  - LA VICTORIA, Tipo: INCENDIO / ESTRUCTURAS / VIVIENDA / MATERIAL NOBLE',500,'actual','emergencia_ciudadana')
	#construir_mensaje(None,'Dirección: AV. GENERAL CESAR CANEVARO  900  - LINCE, Tipo: ACCIDENTE VEHICULAR / PARTICULAR / AUTOMOVIL',200,'frecuente','emergencia_ciudadana')