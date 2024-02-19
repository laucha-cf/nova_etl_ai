"""
Generar Dataframe con Metadata Hive.
Generar Dataframe con mapeo campos RAW+CUR.

"""
import json
import pandas as pd

from functions.obtener_metadata import generar_lista_metadata_hive, generar_lista_metadata_impala
from functions.obtener_metadata import generar_df_metadata

from functions.cleansing_metadata import agregar_comentarios_hive, agregar_fecha_proceso

from functions.mapeo_raw_cur import mapeo_campos_raw_cur



with open('data_consumo/data_cdg.json') as f:
    file = f.read()
    json_file = json.loads(file)

# EL DF SE GUARDARÁ EN LA UBICACIÓN QUE ESPECIFIQUES!!!
path_metadata = 'data_consumo/metadata_hive_cdg.csv'
path_mapeos = 'data_consumo/mapeo_raw_cur.csv'

if __name__ == '__main__':
    #Obtenemos df con mapeo de campos RAW + CUR
    df_campos_raw_cur = mapeo_campos_raw_cur( json_file=json_file )

    #Generamos metadata hive
    lista_metadata_hive = generar_lista_metadata_hive( json_file )

    df_metadata = generar_df_metadata( lista_aplanada=lista_metadata_hive, 
                                        hive=True )
    
    #Agregamos comentarios y arreglamos comments de fecha_proceso
    df_metadata = agregar_comentarios_hive( df=df_metadata.copy() )
    df_metadata = agregar_fecha_proceso( df=df_metadata.copy() )
    


    try:
        df_metadata.to_csv(path=path_metadata, sep='|', index=False)
        df_campos_raw_cur.to_csv(path=path_mapeos, sep='|', index=False)
        print(f'Se guardaron los Dataframes correctamente en: {path_metadata} y {path_mapeos}')
    except Exception as e:
        print('No se ha podido guardar el Dataframe debido al siguiente error:')
        print(e)

