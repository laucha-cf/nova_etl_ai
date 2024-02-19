# main.py
import os
import datetime
import json
import openai
from treelib import Tree
import pandas as pd
from functions.utils import (
    initialize_pinecone, 
    initialize_openai, 
    split_text_by_user_df, 
    extract_joins, 
    extract_tables_from_sp, 
    replace_columns_with_asterisk,
    prompt_traductor_query,
    query,
    metadata_para_base_vec,
    upload_pinecone
)
import tiktoken
from tqdm import tqdm
import csv

# Abro archivo con json, donde cada objeto es una tabla.
file_path = 'data_consumo/data_cdg.json'
with open(file_path, 'r') as file:
    tables_json = json.load(file)

# Abro archivo csv, donde cada fila es el campo de una tabla
AllRaws = []
file_path = 'data_consumo/metadata_cdg_nuevos_comments.csv'
with open(file_path, 'r', encoding='utf-8') as file:
    for line in file:
        row = line.strip().split('|')
        AllRaws.append(row)


# Inicializo Pinecone
index = initialize_pinecone()

# Inicializo OpenAI
client = initialize_openai(openai.OpenAI)

# Preparación de datos
logs = ""
Todas_Tablas = []
Todas_Tablas_Nombres = []
Todas_Tablas_Nombres_raw = []
Todas_Tablas_Nombres_cur = []
TablasFinalesDM = []
TablasMenos4000TokensConJoin = []
encoding = tiktoken.encoding_for_model("gpt-4")
tree = Tree()
trees = []
current_time = datetime.datetime.now()
formatted_time = current_time.strftime("%Y-%m-%d_%H-%M")
Actualizo = True

# Procesamiento de datos
if Actualizo:
    for item in tqdm( tables_json.items() ):
        if "ber" in item[0]:
            if item[1]["loading_query"]:
                Query_upper = item[1]["loading_query"].upper()
                Query_upper = Query_upper.replace("FROM_", "FR_")
            else:
                Query_upper = ""
            # Filtrar las filas que coinciden con el elemento actual
            
            item_upper = [item[0].upper(), {"loading_query": Query_upper, "affected_tables": item[1]["affected_tables"], "Tokens": 0, "Joins":[],"Prompt":"","create_table":item[1]["create_table"],"QueryIdentificado":False, "RowsTable":  [row for row in AllRaws if (row[3] + "." + row[4]).upper() == item[0].upper()],"Prompt":"","Vector":[],"Comments":""}]
            if len(item_upper[1]["affected_tables"]) == 0 and item_upper[0] not in TablasFinalesDM:
                TablasFinalesDM.append(item_upper[0])
            Todas_Tablas.append(item_upper)
            Todas_Tablas_Nombres.append(item_upper[0])
            if "2CUR" in item_upper[0]:
                Todas_Tablas_Nombres_cur.append((item_upper[0]))
else:
    with open("TodasTablas.txt", 'r') as file:
        Todas_Tablas = json.load(file)
    for item in Todas_Tablas:
        if len(item[1]["affected_tables"]) == 0 and item[0] not in TablasFinalesDM:
                TablasFinalesDM.append(item[0])
        Todas_Tablas_Nombres.append(item[0])
        if "2CUR" in item[0]:
            Todas_Tablas_Nombres_cur.append((item[0]))

# Armar árbol y prompt para obtener el select en cur de manera recursiva
TablasCurXTablaDM = []
for Tabla_Inicio_Nombre in  TablasFinalesDM :
    indice_aux = Todas_Tablas_Nombres.index(Tabla_Inicio_Nombre)
    if not Todas_Tablas[indice_aux][1]["QueryIdentificado"]:
        tree = Tree()
        Tabla_inicio = Todas_Tablas[indice_aux]
        tree.create_node(Tabla_Inicio_Nombre, Tabla_Inicio_Nombre) 
        prompt = prompt_traductor_query(Tabla_inicio, tree, Todas_Tablas, Todas_Tablas_Nombres_cur, encoding, TablasCurXTablaDM)
        Tokens = len(encoding.encode(prompt))
        Todas_Tablas[indice_aux][1]["Tokens"] = Tokens
        Todas_Tablas[indice_aux][1]["Prompt"] = prompt            
        logs += "\n" + tree.to_json()
        logs += "\n" + "El Prompt para el query en cur es:" +  "\n" + prompt
        trees.append(tree)
        if "JOIN" in prompt:
            try:
                respuesta = query(prompt, Tabla_Inicio_Nombre, client, encoding)  
                logs += "\n" + str(respuesta)
                respuesta = respuesta.replace("```sql","")
                respuesta = replace_columns_with_asterisk(respuesta)
                joins_tabla = extract_tables_from_sp(respuesta)
                for table in joins_tabla:
                    try:
                        Todas_Tablas[Todas_Tablas_Nombres.index(table)][1]["Joins"].append(respuesta)
                    except:
                        #Me inventa tablas
                        pass

                Todas_Tablas[indice_aux][1]["QueryIdentificado"] = True

                file_path = "logs/log" + formatted_time + ".txt"

                with open(file_path, 'w') as file:
                    file.write(logs)

                file_path = "data_cdg_enriquecida.json"

                if os.path.exists(file_path):
                    os.remove(file_path)

                diccionario_resultante = {}
                # Iterar sobre la lista original
                for elemento in Todas_Tablas:
                    # Obtener el nombre de la tabla y los datos asociados
                    nombre_tabla = elemento[0]
                    datos_tabla = elemento[1]

                    # Dividir el nombre de la tabla en base de datos y nombre de la tabla
                    database, table_name = nombre_tabla.split('.')

                    # Agregar los datos al diccionario resultante
                    diccionario_resultante[nombre_tabla] = {
                            "database": database,
                            "table_name": table_name,
                            "create_table": datos_tabla.get("create_table", ""),
                            "associated_term": datos_tabla.get("associated_term", ""),
                            "loading_query": datos_tabla.get("loading_query", ""),
                            "affected_tables": datos_tabla.get("affected_tables", []),
                            "Tokens": datos_tabla.get("Tokens", 0),
                            "Joins": datos_tabla.get("Joins", []),
                            "Prompt": datos_tabla.get("Prompt", ""),
                            "QueryIdentificado": datos_tabla.get("QueryIdentificado", False),
                            "RowsTable": datos_tabla.get("RowsTable", []),
                            "Vector": datos_tabla.get("Vector", []),
                            "Comments": datos_tabla.get("Comments", "")
                        }

                with open(file_path, 'w') as file:
                    json.dump(diccionario_resultante, file, indent=2)
            except Exception as e:
                print("Error con " + Tabla_Inicio_Nombre)
                print(f"Exception: {str(e)}")
                logs += "\n" + "Error con " + Tabla_Inicio_Nombre + "\n" + f"Exception: {str(e)}"

# Subir embeddings de las tablas finales en cur a Pinecone
#indice_aux = Todas_Tablas_Nombres.index("ber_2cur.tabla_final")
#i = 0
#index = upload_pinecone(index, "tabla_final", i)
#
## Generar metadata para Base Vec
#metadata_para_base_vec(Todas_Tablas)
