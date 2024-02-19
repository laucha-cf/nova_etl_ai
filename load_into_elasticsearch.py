import warnings

# Para ignorar todos los warnings
warnings.filterwarnings("ignore")
from elasticsearch import Elasticsearch

from tqdm import tqdm
import json

import os
from dotenv import load_dotenv

load_dotenv()

# Obtener las credenciales para ElasticSearch
es_address = os.getenv("ES_ADDRESS")
es_port = os.getenv("ES_PORT")
es_scheme = os.getenv("ES_SCHEME")



# Ruta al archivo JSON
path_archivo_json = "data_consumo/data_cdg_enriquecida.json"

with open(path_archivo_json) as f:
    file = f.read()
    json_file = json.loads(file)


if __name__ == '__main__':

    try:
        client = Elasticsearch(f'{es_scheme}://{es_address}:{es_port}')
        print("El servidor Elasticsearch está en funcionamiento.")
    except Exception as e:
        print("El servidor Elasticsearch está en funcionamiento.")
        print(e)
    

    # Indexar todos los datos en un solo índice
    index_name = "datos_cdg"

    for tabla, datos_tabla in tqdm(json_file.items(), desc="Indexando datos"):
        # Indexar los datos en Elasticsearch
        client.index(index=index_name, body=datos_tabla)

    print("Ingesta de datos completada exitosamente.")

