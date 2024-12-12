# Librerias
# Generales
import pandas as pd
import numpy as np
import time
import warnings
import os
import json
import re
# Snowflake
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from snowflake.snowpark import Session


################################
# FUNCIONES PARA EJECUTAR QUERYS
################################

def ejecutar_query(conn, query):
    """
    Ejecuta una consulta SQL y devuelve el primer resultado.

    Parámetros:
    conn (psycopg2.extensions.connection): La conexión a la base de datos.
    query (str): La consulta SQL a ejecutar.

    Retorna:
    tuple: El primer resultado de la consulta.
    """
    # Crear un cursor para ejecutar consultas
    cur = conn.cursor()
    try:
        # Ejecutar la consulta
        cur.execute(query)
        # Obtener el primer resultado
        result = cur.fetchone()
    finally:
        # Cerrar el cursor
        cur.close()
    
    return result

def snowflake_sql(conn, sql):
    """
    Ejecuta un comando SQL en Snowflake y devuelve los resultados en un DataFrame de pandas.

    Parámetros:
    conn (snowflake.connector.connection): La conexión a la base de datos.
    sql (str): La consulta SQL a ejecutar.

    Retorna:
    pandas.DataFrame: Un DataFrame con los resultados de la consulta.
    """
    # Crear un cursor para ejecutar la consulta
    cur = conn.cursor()
    try:
        # Ejecutar la consulta SQL
        cur.execute(sql)
        # Obtener los nombres de las columnas de los resultados de la consulta
        column_names = [desc[0] for desc in cur.description]
        # Obtener todos los resultados de la consulta
        results = cur.fetchall()
        # Crear un DataFrame de pandas con los resultados
        df = pd.DataFrame(results, columns=column_names)
    finally:
        # Cerrar el cursor
        cur.close()
    
    return df

#############################
# MAPEO DE DATOS PYTHON - SQL
#############################

tipo_datos_map = {
    # Tipos numéricos
    'float64': 'FLOAT',    # Numpy/Pandas float
    'int64': 'FLOAT',    # Numpy/Pandas integer
    'float': 'FLOAT',      # Python float
    'int': 'FLOAT',      # Python int

    # Tipos de cadenas
    'object': 'STRING',    # Pandas object, usualmente strings
    'str': 'STRING',       # Python string

    # Tipos booleanos
    'bool': 'BOOLEAN',     # Python boolean
    'bool_': 'BOOLEAN',    # Numpy boolean

    # Tipos de fecha y tiempo
    'datetime64': 'DATETIME', # Numpy/Pandas datetime
    'datetime64[ns]': 'DATETIME', # Numpy/Pandas datetime con precisión de nanosegundos
    'datetime.date': 'DATE',  # Python date
    'datetime.datetime': 'DATETIME',  # Python datetime

    # Tipos de datos estructurados (más complejos)
    'list': 'ARRAY',       # Listas podrían mapearse a arrays en SQL
    'dict': 'JSON',        # Diccionarios podrían mapearse a JSON en SQL
    'tuple': 'ARRAY',      # Tuplas podrían mapearse a arrays en SQL
    'set': 'ARRAY',        # Sets podrían mapearse a arrays en SQL

    # Tipos de datos binarios
    'bytes': 'BLOB'        # Python bytes
}

###########################
# FUNCIÓN PARA CARGAR DATOS
###########################

def snowflake_cargar_df(conn, df, nombre_tabla):
    """
    Carga un DataFrame de pandas en una tabla de Snowflake.

    Parámetros:
    conn (snowflake.connector.connection): La conexión a la base de datos.
    df (pandas.DataFrame): El DataFrame a cargar en la tabla.
    nombre_tabla (str): El nombre de la tabla en la que se cargará el DataFrame.

    Retorna:
    str: Mensajes de estado y resultados del proceso de carga.
    """
    # Crear un cursor para ejecutar consultas
    cur = conn.cursor()
    try:
        # Crear la sentencia SQL para crear la tabla
        create_table_query = f'CREATE OR REPLACE TABLE {nombre_tabla} ('

        # Añadir las columnas y sus tipos basándose en el DataFrame usando el diccionario
        for col, dtype in df.dtypes.items():
            snowflake_type = tipo_datos_map.get(str(dtype), 'STRING')  # Usamos 'STRING' como tipo por defecto si no se encuentra en el mapa
            create_table_query += f"{col} {snowflake_type}, "

        # Remover la última coma y espacio, cerrar el paréntesis y agregar el punto y coma
        create_table_query = create_table_query.rstrip(', ') + ");"

        # Ejecutar la creación de la tabla (con los nombres previamente validados)
        cur.execute(create_table_query)

        # Calcula el uso de memoria total (en bytes) de las columnas y divide por el número de filas para obtener el tamaño promedio por fila
        memory_per_row = df.memory_usage(deep=True).sum() / len(df)
        
        # 32 GB en bytes (50% de la ram)
        memory_allocated = 32 * 1024 ** 3

        # Calcular cuántas filas caben en 32 GB
        chunk_size_recomendado = int(memory_allocated // memory_per_row)

        # Mensajes de la función 
        mensajes = []

        # Cargar base de datos a la tabla definida
        try:
            start_time = time.time()
            success, nchunks, nrows, _ = write_pandas(conn,
                                                      df=df, 
                                                      table_name=f'{nombre_tabla}', 
                                                      quote_identifiers=False,
                                                      chunk_size=chunk_size_recomendado)
            end_time = time.time()

            if success:
                mensajes.append(f"DataFrame cargado exitosamente en la tabla: {nrows} filas en {nchunks} chunks.")
                mensajes.append(f"Tiempo de carga: {end_time - start_time:.2f} segundos.")
            else:
                mensajes.append("Error al cargar el DataFrame en la tabla")
        except Exception as e:
            mensajes.append(f"Se produjo un error durante la carga: {e}")
        finally:
            mensajes.append("Proceso terminado")
    finally:
        # Cerrar el cursor
        cur.close()
    
    return "\n".join(mensajes)


###################
# FUNCIÓN PARA ETLs
###################
def ejecutar_script_sql(conn, sql_script):
    """
    Elimina comentarios y líneas en blanco de un script SQL, lo divide en comandos individuales
    y ejecuta cada comando en la base de datos de Snowflake.

    Parámetros:
    conn (snowflake.connector.connection): La conexión a la base de datos.
    sql_script (str): El script SQL a ejecutar.

    Retorna:
    None
    """
    # Eliminar los comentarios y las líneas en blanco
    sql_script = re.sub(r'--.*', '', sql_script)  # Elimina comentarios
    sql_script = re.sub(r'\s+', ' ', sql_script)  # Elimina líneas en blanco y múltiples espacios
    
    # Dividir el script en comandos individuales
    sql_commands = sql_script.split(';')
    
    # Ejecuta cada comando y muestra el resultado
    for command in sql_commands:
        command = command.strip()
        if command:
            try:
                conn.cursor().execute(command)
                print(f"Ejecutado con éxito: {command[:100]}...")  # Muestra los primeros 100 caracteres del comando
            except Exception as e:
                print(f"Error al ejecutar: {command[:100]}...")
                print(f"Error: {e}")