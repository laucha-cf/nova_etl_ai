"""Obtener CSV con mapeo de campos de RAW y CURADO
"""
from obtener_metadata import describe_table

from requests.auth import HTTPBasicAuth
import pandas as pd

import os
from dotenv import load_dotenv
from requests.auth import HTTPBasicAuth


from tqdm import tqdm

load_dotenv()

# Obtener las credenciales para Atlas
atlas_username = os.getenv("ATLAS_USERNAME")
atlas_password = os.getenv("ATLAS_PASSWORD")

# Obtener las configuraciones para Hive
hive_host = os.getenv("HIVE_HOST")
hive_port = os.getenv("HIVE_PORT")
hive_user = os.getenv("HIVE_USER")

# Obtener las configuraciones para Impala
impala_host = os.getenv("IMPALA_HOST")
impala_port = os.getenv("IMPALA_PORT")
impala_user = os.getenv("IMPALA_USER")

headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
}
credentials = HTTPBasicAuth(atlas_username, atlas_password)
atlas_urls = ["172.30.213.141", "172.30.213.142"]
atlas_port = 31000


def es_garbage( campo, opciones ):
    """Crea un Dataframe con el mapeo de campos de RAW + CUR.

    Params
    campo : Valor del campo actual que se está recorriendo.
    opciones : Lista de valores por verificar.

    Return
    True o False, según corresponda.
    """
    return campo in opciones

def mapeo_campos_raw_cur( json_file, path:str ):
    """Crea un Dataframe con el mapeo de campos de RAW + CUR.

    Params
    json_file : Archivo JSON del cuál obtiene las tablas a mapear.
    path : Ubicación donde queremos guardar el dataframe.

    Return
    None
    """
    options = ['# Partition Information', '# col_name', '']
    pares_raw_cur = []

    for table_raw in tqdm( json_file.keys() ):
        if '1raw' in table_raw:
            db_raw, table_name_raw = table_raw.split('.')
            # Obtenemos lista de tablas afectadas
            list_affected_tables = json_file[table_raw]['affected_tables']
            # Si afecta más de una tabla, debemos obtener solo la que posea la misma fuente en el nombre en curado
            if len(list_affected_tables)>1:
                source = table_name_raw.split('_')[-1]
                # Filtra la tabla que contiene la fuente
                cur_table = [t for t in list_affected_tables if source in t]
                cur_table = cur_table[0]
            if len(list_affected_tables)>0:
                # Si tiene 1 solo elemento, entonces esa es la tabla en curado
                cur_table = list_affected_tables[0]

            # Si llgué acá es porque tengo la tabla en raw + cur
            db_cur, table_name_cur = cur_table.split('.')

            create_raw = describe_table(db_raw, table_name_raw) 
            create_cur = describe_table(db_cur, table_name_cur) 
            if len(create_raw) == len(create_cur):
                #La cantidad de campos es igual
                #Zippeamos campo (raw, cur)
                pares_raw_cur.append( [(f'{table_raw}.{tupla_raw[0]}', f'{cur_table}.{tupla_cur[0]}') for tupla_raw, tupla_cur in zip(create_raw, create_cur) if not es_garbage(tupla_raw[0], options)] )
    
    # Aplanar la lista anidada
    lista_aplanada = [tupla for sublist in pares_raw_cur for tupla in sublist]

    # Crear un DataFrame a partir de la lista aplanada
    df = pd.DataFrame(lista_aplanada, columns=['nombre_campo_raw', 'nombre_campo_cur'])
    try:
        df.to_csv(f'{path}.csv', sep='|', index=False)
        print(f'Se guardó el Dataframe en {path}.csv')
    except Exception as e:
        print('No se ha podido guardar el Dataframe debido al siguiente error:')
        print(e)