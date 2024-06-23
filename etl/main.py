import pandas as pd
import json

def combine_csv_files(desastre_natural_path, conflicto_social_path, siniestros_path, output_path):
    # Leer los CSV
    desastre_natural_df = pd.read_csv(desastre_natural_path)
    conflicto_social_df = pd.read_csv(conflicto_social_path)
    siniestros_df = pd.read_csv(siniestros_path)

    # Crear listas para los datos procesados
    data = []

    # Función para extraer campos del primer CSV (DESASTRE NATURAL)
    def process_desastre_natural_row(row):
        try:
            data_json = json.loads(row['data'])
            latitude = float(data_json.get('latitud', 'NaN'))
            longitude = float(data_json.get('longitud', 'NaN'))
            description = f"Magnitud: {data_json.get('magnitud', 'N/A')}, Referencia: {data_json.get('referencia', 'N/A')}"
            status='null'


            return {
                'latitud_evento': latitude,
                'longitud_evento': longitude,
                'tipo_evento': 'sismo',
                'descripcion': description,
                'ultima_actualizacion': row['updatedAt'],
                'estado_evento':status
            }
        except (json.JSONDecodeError, KeyError, ValueError):
            return None

    # Función para extraer campos del segundo CSV (CONFLICTO SOCIAL)
    def process_conflicto_social_row(row):
        description = f"Nombre: {row['name']}, Departamento: {row['department']}, Provincia: {row['province']}, Distrito: {row['district']}, Tipo: {row['type']}"
        status = 'null'
        if(row['state']!=''):
            if row['state']=='Activo' or row['state']=='Latente':
                status = True
            else:
                status=False
        else:
            status=''

        return {
            'latitud_evento': row['latitude'],
            'longitud_evento': row['longitude'],
            'tipo_evento': 'conflicto_social',
            'descripcion': description,
            'ultima_actualizacion': row['updatedAt'],
            'estado_evento': status
        }

    # Función para extraer campos del tercer CSV (SINIESTROS)
    def process_emergencia_ciudadana_row(row):
        event_type = 'emergencia_ciudadana'
        description = f"Dirección: {row['address']}, Tipo: {row['type']}"
        status='null'
        if(row['state']!=''):
            if row['status']=='ATENDIENDO':
                status = True
            elif row['status']=='CERRADO':
                status=False
        else:
            status=''
        return {
            'latitud_evento': row['lat'],
            'longitud_evento': row['lng'],
            'tipo_evento': event_type,
            'descripcion': description,
            'ultima_actualizacion': row['updatedAt'],
            'estado_evento': status
        }

    # Procesar cada fila de cada DataFrame y agregar al data list
    for index, row in desastre_natural_df.iterrows():
        desastre_data = process_desastre_natural_row(row)
        if desastre_data:
            data.append(desastre_data)

    for index, row in conflicto_social_df.iterrows():
        conflicto_data = process_conflicto_social_row(row)
        data.append(conflicto_data)

    for index, row in siniestros_df.iterrows():
        siniestro_data = process_emergencia_ciudadana_row(row)
        data.append(siniestro_data)

    # Crear DataFrame final
    final_df = pd.DataFrame(data)
    final_df.insert(0, 'id_evento', range(1, len(final_df) + 1))

    # Guardar en un nuevo CSV
    final_df.to_csv(output_path, index=False)

# Llamar la función
combine_csv_files('desastre_natural.csv', 'conflicto_social.csv', 'emergencia_ciudadana.csv', 'final3.csv')
