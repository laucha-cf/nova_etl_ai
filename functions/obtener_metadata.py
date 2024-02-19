"""
El objetivo es obtener toda la Metadata pertinente a Hive o Impala, según corresponda.
"""
from requests.auth import HTTPBasicAuth
from impala.dbapi import connect
from datetime import datetime
from pyhive import hive
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


def describe_table( hive_database, table_name ):
    """Obtiene campos, tipo de dato y comentario de campo de una tabla en Hive.

    Params
    impala_database : Base de datos a referenciar.
    table_name : Nombre de tabla.

    Return
    databases : Metadata de la tabla en formato texto.
    """
    hive_conn = hive.connect(host=hive_host, port=hive_port, username=hive_user, database=hive_database)
    hive_cursor = hive_conn.cursor()
    hive_cursor.execute(f'DESCRIBE {hive_database}.{table_name}')
    databases = hive_cursor.fetchall()
    
    return databases

def describe_table_impala( impala_database, table_name ):
    """Obtiene campos, tipo de dato y comentario de campo de una tabla en Impala.

    Params
    impala_database : Base de datos a referenciar.
    table_name : Nombre de tabla.

    Return
    databases : Metadata de la tabla en formato texto.
    """
    impala_conn = connect(host=impala_host, port=impala_port, database=impala_database)
    impala_cursor = impala_conn.cursor()
    impala_cursor.execute(f'DESCRIBE {impala_database}.{table_name}')
    databases = impala_cursor.fetchall()
    
    return databases

def generar_lista_metadata_hive( json_file ):
    """Genera una lista con metadata de Hive.

    Params
    json_file : Archivo JSON con metadata.

    Return
    lista_aplanada : Lista con metadata.
    """
    lista_metadata = []

    for table in tqdm(json_file.keys()):
        if '4con' not in table and 'datamart' not in table:
            db, table_name = table.split('.')
            describe = describe_table( db, table_name )
            db_split = db.split('_')
            entidad = db_split[1]
            if 'datamart' in db:
                zona = 'datamart'
            else:
                zona = db_split[2][1:]

            a = list(map(lambda x: list(x)+[db, table_name, entidad, zona], describe ))
            lista_metadata.append(a)

    lista_aplanada = [item for sublist1 in lista_metadata for item in sublist1]

    return lista_aplanada

def generar_lista_metadata_impala( json_file ):
    """Genera una lista con metadata de Impala.

    Params
    json_file : Archivo JSON con metadata.

    Return
    lista_aplanada_impala : Lista con metadata.
    """
    lista_metadata_impala = []

    for table in tqdm(json_file.keys()):
        if '1raw' not in table and '2cur' not in table and '3ref' not in table:
            db, table_name = table.split('.')
            describe = describe_table_impala( db, table_name )
            db_split = db.split('_')
            entidad = db_split[1]
            if 'datamart' in db:
                zona = 'datamart'
            else:
                zona = db_split[2][1:]
            a = list(map(lambda x: list(x[:4])+[db, table_name, entidad, zona], describe ))
            lista_metadata_impala.append(a)
    
    lista_aplanada_impala = [item for sublist1 in lista_metadata_impala for item in sublist1]
    return lista_aplanada_impala

def generar_df_metadata( lista_aplanada:list, hive:True ):
    """Genera y guarda el Dataframe en la ubicación indicada.

    Params
    lista_aplanada : Lista con metadata de hive.
    hive : Indica si la metadata se corresponde a Hive o Impala. Por defecto en Hive.

    Return
    df_metadata : Dataframe con metadata de hive o impala, segun corresponda.
    """
    if hive:
        nombres_columnas = ['campo', 'tipo_dato', 'comentario', 'base','tabla','entidad','zona']
        df_metadata = pd.DataFrame(lista_aplanada, columns=nombres_columnas)
        options = ['# Partition Information', '# col_name', ''] 
        df_metadata = df_metadata.loc[~df_metadata['campo'].isin(options)]
    else:
        #Impala
        nombres_columnas_impala = ['campo', 'tipo_dato', 'comentario','primary_key', 'base','tabla','entidad','zona']
        df_metadata = pd.DataFrame(lista_aplanada, columns=nombres_columnas_impala)

    return df_metadata

