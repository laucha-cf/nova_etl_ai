"""
Obtener toda la data necesaria para el modelo LLM.

"""
import json
import pandas as pd

from obtener_metadata import generar_lista_metadata_hive, generar_lista_metadata_impala
from obtener_metadata import generar_df_metadata

from create_table_json import agregar_create_table

from cleansing_metadata import agregar_comentarios_hive, agregar_fecha_proceso

from mapeo_raw_cur import mapeo_campos_raw_cur


#Levantamos archivo sin CREATE TABLES
with open('data_cruda/data_prestamos.json') as f:
    file = f.read()
    json_file = json.loads(file)

#df = pd.read_csv('data_procesada/metadata_hive_prestamos.csv', sep='|', index_col=False)

if __name__ == '__main__':

    mapeo_campos_raw_cur( json_file=json_file, path='data_consumo/FUNCIONA_mapeo')
    """
    #Agregamos comentarios a la metadata
    agregar_comentarios_hive( df=df, path='data_procesada/metadata_con_comentarios' )

    #Obtenemos y guardamos el json con los CREATE TABLE
    agregar_create_table( json_file=json_file, path='data_procesada/AGREGUE_LOS_CREATE' )

    #Generamos metadata hive
    lista_metadata_hive = generar_lista_metadata_hive( json_file )

    generar_df_metadata( lista_aplanada=lista_metadata_hive, 
                         path='data_procesada/metadata_hive_prestamos',
                         hive=True )

    #Generamos metadata impala
    lista_metadata_impala = generar_lista_metadata_impala( json_file )

    generar_df_metadata( lista_aplanada=lista_metadata_impala, 
                         path='data_procesada/METADATA_IMPALA_FUNCIONA',
                         hive=False )
    """

