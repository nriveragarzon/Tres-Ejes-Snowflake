# Librerias
from snowflake.snowpark import Session
import pandas as pd
import numpy as np

###############################################################
# FUNCIONES PARA GENERAR LAS OPCIONES DE ELECCIÓN PARA USUARIOS
###############################################################

# Selector de continentes
def selector_continentes(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de continentes distintos
    desde la correlativa de continentes en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de continentes distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base, manteniendo la estructura simple y directa
    query = """
    SELECT DISTINCT A.REGION_NAME
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.CONTINENTES AS A
    WHERE A.REGION_NAME NOT IN ('Antártida', 'No definido', 'Sin Especificar', 'No Declarados')
    """

    # 2. Ejecutar la consulta SQL y recoger resultados. La consulta ya está ordenada en SQL, por lo que no necesitamos ordenarla luego.
    data = session.sql(query).collect()

    # 3. Convertir los resultados en un set directamente para eliminar duplicados y optimizar memoria
    opciones = {row['REGION_NAME'] for row in data}

    # 4. Convertimos el set en una lista ordenada
    return sorted(opciones)


# Selector de tlcs
def selector_tlcs(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de tlcs distintos
    desde la correlativa de TLCs en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de tlcs distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base, eliminando la ordenación en la consulta SQL para optimizar el rendimiento
    query = """
    SELECT DISTINCT A.NOMBRE_TLC
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.TLCS AS A
    WHERE A.NOMBRE_TLC NOT IN ('En Curso', 'No Declarados', 'Resto de países')
    """

    # 2. Ejecutar la consulta SQL y recoger los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Usar un conjunto para eliminar duplicados y reducir el uso de memoria y procesamiento
    opciones = {row['NOMBRE_TLC'] for row in data}

    # 4. Convertir el conjunto en una lista ordenada
    return sorted(opciones)


# Selector de HUBS
def selector_hubs(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de hubs distintos
    desde la correlativa de Hubs en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de hubs distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base sin la cláusula ORDER BY para optimizar el rendimiento
    query = """
    SELECT DISTINCT A.NOMBRE_HUB
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.HUBS AS A
    WHERE A.NOMBRE_HUB NOT IN ('Colombia', 'Otros')
    """

    # 2. Ejecutar la consulta SQL y recoger los resultados
    data = session.sql(query).collect()

    # 3. Utilizar un conjunto para eliminar duplicados y optimizar el uso de memoria
    opciones = {row['NOMBRE_HUB'] for row in data}

    # 4. Convertir el conjunto en una lista ordenada alfabéticamente
    return sorted(opciones)


# Selector de continentes para paises
def selector_continentes_paises(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de continentes distintos
    desde la correlativa de continentes en Snowflake y los devuelve como una lista de opciones ordenada para luego usarlos como selectores de países.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de continentes distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base sin la cláusula ORDER BY para optimizar el rendimiento
    # y filtrando continentes directamente en la consulta
    query = """
    SELECT DISTINCT A.REGION_NAME
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.CONTINENTES AS A
    WHERE A.REGION_NAME NOT IN ('Antártida', 'No definido', 'Sin Especificar', 'No Declarados')
    """

    # 2. Ejecutar la consulta SQL y recoger los resultados
    data = session.sql(query).collect()

    # 3. Utilizar un conjunto para eliminar duplicados y optimizar el uso de memoria
    opciones = {row['REGION_NAME'] for row in data}

    # 4. Convertir el conjunto en una lista ordenada alfabéticamente
    return sorted(opciones)



# Selector de países
def selector_paises(session, continentes):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de países distintos
    desde la correlativa de países en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.
    - continentes: lista de strings con los continentes seleccionados para filtrar los países de interés.

    Retorna:
    - opciones: Lista de países distintos ordenada alfabéticamente.
    """
    # 0. Transformar lista de continentes 
    continente_pais_list = [continentes] if continentes else []

    # 1. Construir la consulta SQL base
    query = """
    SELECT DISTINCT A.COUNTRY_OR_AREA
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES AS A
    WHERE A.COUNTRY_OR_AREA IS NOT NULL AND A.COUNTRY_OR_AREA NOT IN ('No declarado', 'No definido', 'Organismos internacionales', 'Otros', 'PAÍS NO INCLUIDO')
    """
    # Agrupación geográfica: 
    if continentes:
        query += f""" AND A.REGION_NAME IN ({','.join([f"'{continente}'" for continente in continente_pais_list])});"""

    # 2. Ejecutar la consulta SQL y convertir los resultados en un DataFrame de pandas
    data = session.sql(query).collect()

    # 3. Convertir los resultados en una lista de opciones ordenada
    opciones = sorted({row['COUNTRY_OR_AREA'] for row in data})

    # 4. Devolver la lista de opciones
    return opciones


# Selector de departamentos
def selector_departamento(session):
    """
    Esta función ejecuta una consulta SQL para obtener una lista de departamentos distintos
    desde la base de datos de exportaciones en Snowflake y los devuelve como una lista de opciones ordenada.

    Parámetros:
    - session: objeto de conexión activo a Snowflake.

    Retorna:
    - opciones: Lista de departamentos distintos ordenada alfabéticamente.
    """

    # 1. Construir la consulta SQL base, eliminando la cláusula ORDER BY para optimizar el rendimiento
    query = """
    SELECT DISTINCT A.DEPARTAMENTO_DIAN
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIAN_DEPARTAMENTOS AS A
    WHERE A.DEPARTAMENTO_DIAN NOT IN ('Desconocido', 'Sin especificar')
    """

    # 2. Ejecutar la consulta SQL y recoger los resultados
    data = session.sql(query).collect()

    # 3. Usar un conjunto para optimizar la eliminación de duplicados y gestionar la memoria
    opciones = {row['DEPARTAMENTO_DIAN'] for row in data}

    # 4. Devolver la lista de opciones ordenada alfabéticamente
    return sorted(opciones)
