""" Agregar los CREATE TABLE a todas las tablas dentro del JSON.
"""
from impala.dbapi import connect
from datetime import datetime
from pyhive import hive
import pandas as pd
import json


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


def show_create_table_impala( impala_database, table_name ):
    """Obtiene el resultado de la sentencia 'SHOW CREATE TABLE'
       para la tabla indicada en Impala.

    Params
    hive_database : Base de datos requerida.
    table_name : Nombre de la tabla a obtener los datos.

    Return
    create_table_query : Resultado de la sentencia en formato texto.
    """
    impala_conn = connect(host=impala_host, port=impala_port, database=impala_database)
    impala_cursor = impala_conn.cursor()
    impala_cursor.execute(f'SHOW CREATE TABLE {impala_database}.{table_name}')
    databases = list(map(lambda x: x[0], impala_cursor.fetchall()))
    create_table_query = " ".join(databases)

    return create_table_query

def show_create_table( hive_database, table_name ):
    """Obtiene el resultado de la sentencia 'SHOW CREATE TABLE'
       para la tabla indicada en Hive.

    Params
    hive_database : Base de datos requerida.
    table_name : Nombre de la tabla a obtener los datos.

    Return
    create_table_query : Resultado de la sentencia en formato texto.
    """
    hive_conn = hive.connect(host=hive_host, port=hive_port, username=hive_user, database=hive_database)
    hive_cursor = hive_conn.cursor()
    hive_cursor.execute(f'SHOW CREATE TABLE {hive_database}.{table_name}')
    databases = list(map(lambda x: x[0], hive_cursor.fetchall()))
    create_table_query = " ".join(databases)

    return create_table_query

def agregar_create_table( json_file ):
    """Agrega el CREATE TABLE de cada tabla al archivo JSON y 
        lo retorna.

    Params
    json_file : Archivo JSON sin CREATE TABLES.

    Return
    json_file : Archivo JSON con los CREATE TABLES.
    """

    for table in tqdm(json_file.keys(), "AÑADIR CREATE TABLE HIVE"):
        if '4con' not in table and 'datamart' not in table:
            db, table_name = table.split('.')
            table_create_with_properties = show_create_table( db, table_name )
            table_create = table_create_with_properties.split('TBLPROPERTIES')[0]

            json_file[table]['create_table'] = table_create

    for table in tqdm(json_file.keys(), "AÑADIR CREATE TABLE IMPALA"):
        if '1raw' not in table and '2cur' not in table and '3ref' not in table:
            db, table_name = table.split('.')
            table_create_with_properties = show_create_table_impala( db, table_name )

            json_file[table]['create_table'] = table_create_with_properties
    
    return json_file