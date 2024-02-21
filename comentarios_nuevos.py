"""
Reemplazar por los comentarios nuevos en el Dataframe con Metadata.

"""
import json
import pandas as pd
from functions.reemplazar_comentarios_nuevos import ( obtener_diccionario_raw_cur, 
                                                      reemplazar_por_comentarios_nuevos )



with open('data_consumo/data_cdg.json') as f:
    file = f.read()
    json_file = json.loads(file)

path = 'data_consumo/metadata_hive_cdg_nuevos_comentarios.csv'

if __name__ == '__main__':
    # Actualizamos comentarios de tabla a partir de los cambios de @Ana
    diccionario_raw_cur_cdg = obtener_diccionario_raw_cur( json_file=json_file )
    df_metadata = reemplazar_por_comentarios_nuevos( diccionario_raw_cur_cdg )


    try:
        df_metadata.to_csv(path, sep='|', index=False)
        print(f'Se guardó el Dataframe corresctamente')
    except Exception as e:
        print('No se ha podido guardar el Dataframe debido al siguiente error:')
        print(e)

