"""
Agregar CREATE TABLEs dentro del JSON.

"""
import json

from functions.create_table_json import agregar_create_table



with open('data_procesada/data_cdg_sin_comentarios.json') as f:
    file = f.read()
    json_file = json.loads(file)

# EN ESTE PATH SE GUARDA EL JSON CON CREATES!!!
path = 'data_consumo/data_cdg.json'

if __name__ == '__main__':
    #Obtenemos y guardamos el json con los CREATE TABLE
    json_file = agregar_create_table( json_file=json_file )


    try:
        with open(path, 'w') as file:
            json.dump(json_file, file)
        print(f'Se guardó el Dataframe corresctamente en: {path}')
    except Exception as e:
        print('No se ha podido guardar el Dataframe debido al siguiente error:')
        print(e)

