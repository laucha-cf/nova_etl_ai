"""
Reemplazar por los comentarios nuevos en el Dataframe con Metadata.

"""
import json
import pandas as pd
import pickle
from functions.reemplazar_comentarios_nuevos import ( obtener_diccionario_raw_cur, 
                                                      reemplazar_por_comentarios_nuevos )



with open('data_consumo/data_cdg.json') as f:
    file = f.read()
    json_file = json.loads(file)

path_csv = 'data_consumo/metadata_hive_cdg_nuevos_comentarios.csv'

path_dicc = 'data_consumo/diccionario_raw_cur.pkl'

if __name__ == '__main__':
    creo_diccionario = False

    if creo_diccionario:
        diccionario_raw_cur_cdg = obtener_diccionario_raw_cur( json_file=json_file )
        try:
            with open(path_dicc, 'wb') as archivo:
                pickle.dump(diccionario_raw_cur_cdg, archivo)
            print(f'Se guardó el Diccionario correctamente')
        except Exception as e:
            print('No se ha podido guardar el Diccionario debido al siguiente error:')
            print(e)
    else:
        # Cargar el diccionario desde el archivo
        with open(path_dicc, 'rb') as archivo:
            mi_diccionario = pickle.load(archivo)
        
        #Generamos el dataframe con la metadata
        df_metadata = reemplazar_por_comentarios_nuevos( mi_diccionario )

        try:
            df_metadata.to_csv(path_csv, sep='|', index=False)
            print(f'Se guardó el Dataframe correctamente')
        except Exception as e:
            print('No se ha podido guardar el Dataframe debido al siguiente error:')
            print(e)

