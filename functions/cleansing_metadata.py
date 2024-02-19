""" Agregar comentarios de tabla en la metadata y limpiar los comentarios de fecha_proceso.
"""
from functions.create_table_json import show_create_table, show_create_table_impala
import sqlparse
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


#HIVE
def obtener_comentario_tabla_hive( query ):
    """Obtener el comentario de una tabla a partir 
       del SHOW CREATE TABLE.

    Params
    query : SHOW CREATE TABLE de una tabla.

    Return
    comentario_tabla : Comentario de una tabla en formato texto.
    """
    parsed = sqlparse.parse(query)

    for stmt in parsed:
        # Buscar la estructura del CREATE TABLE
        if stmt.get_type() == 'CREATE':
            for token in stmt.tokens:
                if token.value.upper() == 'TABLE':
                    # Encontrar el índice del token TABLE
                    table_index = stmt.token_index(token)

                    # Buscar el comentario de la tabla
                    for i in range(table_index + 1, len(stmt.tokens)):
                        if stmt.tokens[i].value.upper() == 'COMMENT':
                            comentario_tabla = stmt.tokens[i + 2].value.strip("'")
                            return comentario_tabla

    return ''

def agregar_comentarios_hive( df:pd.DataFrame ):
    """Agrega comentarios de tabla al Dataframe de metadata en hive.

    Params
    df : DataFrame con metadata de hive.

    Return
    df_modified : Dataframe con los comentarios modificados.
    """
    df['registro'] = (df['tabla'] != df['tabla'].shift()).cumsum()
    modified_dfs = []

    # Iterar sobre grupos y agregar registros según la condición
    for name, group in tqdm( df.groupby('registro'), 'AGREGAR COMENTARIO TABLA EN METADATA' ):
        create_table = show_create_table(group['base'].iloc[0], group['tabla'].iloc[0])
        comentario_tabla = obtener_comentario_tabla_hive(create_table)

        new_row = pd.DataFrame({'campo': [None], 'tipo_dato': [None], 'comentario': [comentario_tabla],
                                'base': [group['base'].iloc[0]], 'tabla': [group['tabla'].iloc[0]], 'entidad': [None], 'zona': [None]})

        # Concatenar el grupo + row comment
        modified_group = pd.concat([group, new_row], ignore_index=True)
        modified_dfs.append(modified_group)
    
    # Concatenar todos los DataFrames modificados en uno solo
    df_modified = pd.concat(modified_dfs, ignore_index=True)
    df_modified = df_modified.drop(columns=['registro'])

    return df_modified

def procesar_fecha_proceso(fila):
    """Devuelve 'Fecha de Proceso de archivo' si la fila es 'fecha_proceso'
       para todo el df.
    
    Params
    fila : Una row del Dataframe.

    Return
    Comentario actual o 'Fecha de Proceso de archivo', según corresponda.
    """
    if fila['campo']=='fecha_proceso':
        return 'Fecha de Proceso de archivo'
    else:
        return fila['comentario']

def agregar_fecha_proceso( df:pd.DataFrame ):
    """Reemplaza los comentarios de fecha_proceso.

    Params
    df : Dataframe con metadata.

    Return
    df : Dataframe metadata con fecha_proceso.
    """
    df['comentario'] = df.apply(procesar_fecha_proceso, axis=1)
    return df