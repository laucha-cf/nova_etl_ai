# utils.py
import re
import time
import pandas as pd
import pinecone
import sqlparse
import numpy as np
import openai

import os
from pinecone import Pinecone, ServerlessSpec
import time


def get_embedding(text, client, model="text-embedding-ada-002"):
    text = text.replace("\n", " ")
    text = re.sub(r"[^a-zA-Z0-9,':=.\-_\sàèéìíòóùúäëïöü]", ' ', text)
    try:
        Aux = client.embeddings.create(input=[text], model=model)
        vector = Aux.data[0].embedding
    except:
        vector = np.zeros(1536)
        print("Error al intentar vectorizar: " + text)
        # Logs = Logs + " - " + "Error al intentar vectorizar: " + text + "\n" + "----------------------------------------------------------" + "\n"
    return vector


def initialize_pinecone():
    pc = Pinecone(
        api_key="a1d6fbf8-0622-4144-afef-239975f5c984"
    )
    if 'openai' not in pc.list_indexes().names():
        pc.create_index(
            name='openai', 
            dimension=1536, 
            metric='euclidean',
            spec=ServerlessSpec(
                cloud='gcp',
                region='us-west1'
            )
        )
        time.sleep(15) # Esperar un momento para que el índice se cree completamente
    return pc.Index('openai')


def upload_pinecone(index, Core, i):
    global Todas_Tablas
    global Logs
    for tabla in Todas_Tablas:
        if "2CUR" in tabla[0]:
            i += 1
            tabla[1]["Vector"] = get_embedding(tabla[1]["Comments"], model='text-embedding-ada-002')
            Final_List = []
            Aux_list = []
            Aux_list.append(str(i))
            Aux_list.append(tabla[1]["Vector"])
            Aux_list.append({"Core": Core, "Texto":tabla[1]["Prompt"], "tabla": tabla[0]})
            Aux_Tuple = tuple(Aux_list)
            Final_List.append(Aux_Tuple)
            try:
                index.upsert(Final_List)
            except:
                print("No fue posible el upsert en pinecone para:")
                print(Aux_Tuple)
                Logs = Logs + " - " + "Error con upsert " + str(Aux_Tuple) + "\n" + "-------------------------------------------------------------------" + "\n"
    #demora en insertarlo:  
    time.sleep(15)      
    return pinecone.Index('openai')



def split_text_by_user_df(text):
    data = {'Texto': []}
    current_chunk = ""
    for line in text.split('\n'):
        if "User:" in line:
            if current_chunk:
                data['Texto'].append(current_chunk.strip())
            current_chunk = line
        else:
            current_chunk += " " + line
    if current_chunk:
        data['Texto'].append(current_chunk.strip())
    return pd.DataFrame(data)


def extract_joins(sql_query):
    parsed_query = sqlparse.parse(sql_query)
    joins = []
    for statement in parsed_query:
        for token in statement.tokens:
            if "=" in token.value:
                joins.append(token.value)
    return joins


def initialize_openai(OpenAI):
    openai.api_key = "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"
    return OpenAI(api_key="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")


def extract_tables_from_sp(sp_code):
    sp_code = re.sub(r"/\*.*?\*/", "", sp_code, flags=re.DOTALL)
    sp_code = sp_code.replace("\\r", " ")
    table_names = re.findall(r"(?:FROM|JOIN|UPDATE|INTO)\s+([A-Za-z0-9_\.]+)", sp_code, flags=re.IGNORECASE)
    return list(set(table_names))


def replace_columns_with_asterisk(query):
    pattern = re.compile(r'\bSELECT\b(.*?)\bFROM\b', re.IGNORECASE | re.DOTALL)
    modified_query = re.sub(pattern, r'SELECT * FROM', query)
    return modified_query


# utils.py

def prompt_traductor_query(tabla, tree, Todas_Tablas, Todas_Tablas_Nombres_cur, encoding, TablasCurXTablaDM):
    Todas_Tablas_Nombres = [tabla[0] for tabla in Todas_Tablas]  # Definir localmente
    if (tabla[0]) not in Todas_Tablas_Nombres_cur:
        prompt = "La Tabla " + tabla[0] + " se actualiza con la consulta: " +  tabla[1]["loading_query"] + "\n" 
        list_hijos = extract_tables_from_sp(tabla[1]["loading_query"])
        for hijo in list_hijos:
            if hijo in Todas_Tablas_Nombres and hijo != tabla[0]:
                hijo_table = Todas_Tablas[Todas_Tablas_Nombres.index(hijo)]
                if not tree.contains(hijo):
                    tree.create_node(hijo, identifier=hijo, parent=tabla[0]) 
                    prompt += prompt_traductor_query(hijo_table, tree, Todas_Tablas, Todas_Tablas_Nombres_cur, encoding, TablasCurXTablaDM)
                else:
                    hijo_id = hijo + "_BIS"
                    while True:
                        try:
                            tree.create_node(hijo + "_BIS", identifier=hijo_id, parent=tabla[0], data={"Query": tabla[1]["loading_query"]}) 
                            break
                        except:
                            hijo_id += "_BIS"
        tokens = len(encoding.encode(prompt))
        indice_aux = Todas_Tablas_Nombres.index(tabla[0])
        Todas_Tablas[indice_aux][1]["Tokens"] = tokens
        Todas_Tablas[indice_aux][1]["Prompt"] = prompt
        return prompt
    else: 
        TablasCurXTablaDM.append(tabla[0])
        return ""


def query(prompt, Tabla_Inicio_Nombre, client, encoding):
    query = "Eres un especialista en modelado de datos para un banco y especialista en SQL. \n Considerando: " + prompt + ".  Identificar la tabla " + Tabla_Inicio_Nombre + " como una consulta solamente sobre las tablas de la base de_ber_2cur. Solo utilizar las tablas en las consultas, no crear nuevas tablas en de_ber_2cur. Lo que necesito es la traducción de los campos en de_ber_4con a un query sobre de_ber_2cur. Evitar una consulta compleja y consultas anidadas, pero si no hay otra opción incluirlas. Por ejemplo evitar consultas asi: Select * From (select * from A left join B on A.AB1 = B.BA1 ) AB left join C on AB.ABC1 = C.CAB1 y en lugar de eso, escribirla asi: Select * from A left join B on A.AB1 = B.BA1 left join C on A.ABC1 = C.CAB1 donde no hay queries anidados. Responder solo con el código sql sin explicaciones y con todos los campos y uniones necesarias" 
    response = client.chat.completions.create(
        model="gpt-4-turbo-preview",
        temperature=0.2,
        top_p=0.1,
        presence_penalty=0,
        frequency_penalty=0,
        messages=[
            {"role": "user", "content": query}
        ]
    )
    return response.choices[0].message.content


def metadata_para_base_vec(Todas_Tablas):
    prompt = ""
    prompttodo = ""
    descripción_tabla = ""
    for tabla in Todas_Tablas:
        campos = ""
        for campo in tabla[1]["RawsTable"]:
            if campo[0] == "":
                if campo[2] == "" or campo[2] == "''" or len(campo[2]) < 5:
                    descripción_tabla = " Esta es la tabla " + tabla[0]
                else:
                    descripción_tabla = campo[2]
            else:
                campos += " - " + campo[2]
        if "2CUR" in tabla[0]:
            prompt =  "La tabla: " + tabla[0] + " cuya descripción es " + descripción_tabla + " y posee los campos: " + campos + ". Ademas fue creada con el comando: \n" + tabla[1]["create_table"]
            if len(tabla[1]["Joins"]) > 0:
                prompt += "\n Adicionalmente en otras consultas fue relacionada con otras tablas de la siguiente manera: \n"
                for query in tabla[1]["Joins"]:
                    prompt += query + " \n"
            prompttodo += " \n User: " + prompt
            tabla[1]["Prompt"] = prompt
            tabla[1]["Comments"] = "La tabla: " + tabla[0] + " cuya descripción es" + descripción_tabla + " posee los campos: " + campos
    file_path = "Metadata.txt"
    with open(file_path, 'w', encoding='utf-8') as file:
        file.write(prompttodo)
