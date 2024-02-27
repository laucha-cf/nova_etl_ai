import pandas as pd
import re

from tqdm import tqdm

from functions.obtener_metadata import describe_table


path_metadata = 'data_consumo/metadata_hive_cdg_completo.csv'
path_excel = 'data_excel/mejoras_aniv2.csv'

def obtener_diccionario_raw_cur( json_file ):
    """ Obtenemos un diccionario con el formato:
        {
            "tabla_raw": "tabla_cur"
        }

    Params
    json_file : Archivo JSON a recorrer.

    Return
    pares_raw_cur : Diccionario con el formato especificado anteriormente.
    """
    pares_raw_cur = {}

    for table_raw in tqdm( json_file.keys(), desc='GENERANDO LISTA RAW CUR' ):
        if '1raw' in table_raw:
            db_raw, table_name_raw = table_raw.split('.')
            # Obtenemos lista de tablas afectadas
            list_affected_tables = json_file[table_raw]['affected_tables']
            # Si afecta más de una tabla, debemos obtener solo la que posea la misma fuente en el nombre en curado
            if len(list_affected_tables)>1:
                source = table_name_raw.split('_')[-1]
                # Filtra la tabla que contiene la fuente
                cur_table = [t for t in list_affected_tables if source in t]
                if len(cur_table)==0:
                    continue
                cur_table = cur_table[0]
            if len(list_affected_tables)==1:
                # Si tiene 1 solo elemento, entonces esa es la tabla en curado
                cur_table = list_affected_tables[0]

            #cur_table puede ser una lista o un único elemento
            pares_raw_cur[table_raw] = cur_table
            """
            # Si llegué acá es porque tengo la tabla en raw + cur
            db_cur, table_name_cur = cur_table.split('.')

            create_raw = describe_table(db_raw, table_name_raw) 
            create_cur = describe_table(db_cur, table_name_cur) 
            if len(create_raw) == len(create_cur):
                #La cantidad de campos es igual
                # Unimos tablas raw + cur
                pares_raw_cur[table_raw] = cur_table
            """
    
    return pares_raw_cur

def reemplazar_comment_curado( pares_raw_cur, df_metadata, comentario_tabla, nombre_tabla ):
    tabla_curado = pares_raw_cur[nombre_tabla]
    cond_cur = (df_metadata['base_tabla'] == tabla_curado) & (df_metadata['campo'].isna())
    df_metadata.loc[cond_cur, 'comentario'] = comentario_tabla
    return df_metadata


def reemplazar_por_comentarios_nuevos( pares_raw_cur:dict ) -> pd.DataFrame:
    """ Reemplazamos el comentario de tabla si existen modificaciones.

    Params
    pares_raw_cur : Diccionario de mapeos entre tablas raw + cur.

    Return
    pares_raw_cur : Diccionario con el formato especificado anteriormente.
    """
    df_mejoras = pd.read_csv(path_excel)
    df_metadata = pd.read_csv(path_metadata, sep='|', index_col=False)
    df_metadata['base_tabla'] = df_metadata['base'] +'.'+df_metadata['tabla']

    for index, row in tqdm( df_mejoras.iterrows(), desc='REEMPLAZAR POR COMENTARIOS NUEVOS' ):
        # Nombre de tabla a reemplazar en metadata
        nombre_tabla = row['Base.Tabla']

        # Obtengo el comentario nuevo
        create_table_nuevo = row['Create Table Nuevo']

        try:
            comentario_nuevo = create_table_nuevo.split('COMMENT')[-1].replace(';', '')
        except (AttributeError, IndexError) as e:
            # Existen filas vacías o el índice no existe
            continue

        texto = re.sub(r"[\"']", "", comentario_nuevo)
        texto = re.sub(r"\n", "", texto)


        if texto:
            comentario_tabla = texto
        else:
            comentario_tabla = comentario_nuevo
            
        # Reemplazamos en RAW por el nuevo comentario
        cond_raw = (df_metadata['base_tabla'] == nombre_tabla) & (df_metadata['campo'].isna())
        df_metadata.loc[cond_raw, 'comentario'] = comentario_tabla

        #Obtengo la misma tabla en curado
        try:
            df_metadata = reemplazar_comment_curado( pares_raw_cur, 
                                        df_metadata, 
                                        comentario_tabla, 
                                        nombre_tabla )

        except KeyError as e:
            continue

    df_metadata = df_metadata.drop(columns=['base_tabla'])
    
    return df_metadata