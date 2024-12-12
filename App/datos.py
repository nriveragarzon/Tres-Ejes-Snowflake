# Librerias
import pandas as pd
import numpy as np
import numbers
from decimal import Decimal
# import snowflake.connector # [pip install snowflake-connector-python]
from snowflake.connector.pandas_tools import write_pandas # [pip install "snowflake-connector-python[pandas]"]
from snowflake.snowpark import Session

######################################################
# FUNCIONES PARA OBTENER Y TRANSFORMAR DATOS TRES EJES
######################################################

def get_data_parametros(session, agrupacion, continentes=None, paises=None, hubs=None, tlcs=None, departamentos=None, umbral=None):
    """
    Extrae datos desde Snowflake aplicando filtros específicos y devuelve nombres de columnas para usar como parámetros en consultas posteriores.

    Parámetros:
    - session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    - agrupacion (str): Nivel de agrupación para obtener los parámetros: 'CONTINENTES', 'PAISES', 'HUBS', 'TLCS', 'DEPARTAMENTOS' o 'COLOMBIA'.
    - continentes (list): Lista de continentes para filtrar.
    - paises (list): Lista de países para filtrar.
    - hubs (list): Lista de hubs para filtrar.
    - tlcs (list): Lista de tratados de libre comercio para filtrar.
    - departamentos (list): Lista de departamentos para filtrar.
    - umbral (list): Lista con el valor de umbral para contar empresas.

    Retorna:
    - dict: Diccionario con los parámetros necesarios para consultas posteriores.
    """

    # Verificar que los parámetros son listas o None
    # Recorre los nombres y valores de los parámetros que deben ser listas
    for param_name, param in zip(['continentes', 'paises', 'hubs', 'tlcs', 'departamentos'],
                                 [continentes, paises, hubs, tlcs, departamentos]):
        # Si el parámetro no es None y no es una lista, lanza un error
        if param is not None and not isinstance(param, list):
            raise ValueError(f"El parámetro '{param_name}' debe ser una lista o None")

    # Retornar parámetros según la agrupación seleccionada

    # Caso 'COLOMBIA': retorna solo la agrupación porque incluye todas las bases de los tres ejes
    if agrupacion == 'COLOMBIA':
        return {'AGRUPACION': agrupacion, 'UNIDAD': [agrupacion], 'UMBRAL': umbral}

    # Caso 'DEPARTAMENTOS'
    elif agrupacion == 'DEPARTAMENTOS':
        # Verifica que se haya proporcionado el parámetro 'departamentos'
        if not departamentos:
            raise ValueError("El parámetro 'departamentos' es requerido cuando 'agrupacion' es 'DEPARTAMENTOS'")

        # Obtener datos de departamentos desde la tabla en Snowflake
        df_dept = session.table('DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIAN_DEPARTAMENTOS').select(
            'COD_DIAN_DEPARTAMENTO',  # Código del departamento según DIAN
            'DEPARTAMENTO_DIAN'       # Nombre del departamento según DIAN
        )
        # Filtrar los departamentos según la lista proporcionada
        df_dept = df_dept.filter(df_dept['DEPARTAMENTO_DIAN'].isin(departamentos))

        # Convertir el DataFrame de Snowflake a un DataFrame de pandas para manipulación
        data_dept_df = df_dept.to_pandas()

        if not data_dept_df.empty:
            # Extraer nombres y códigos únicos de los departamentos
            unidad = data_dept_df['DEPARTAMENTO_DIAN'].dropna().unique().tolist()
            unidad_cod = data_dept_df['COD_DIAN_DEPARTAMENTO'].dropna().unique().tolist()

            # Obtener municipios asociados a los departamentos seleccionados
            df_mun = session.table('DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIVIPOLA_DEPARTAMENTOS_MUNICIPIOS').select(
                'COD_DANE_DEPARTAMENTO',  # Código DANE del departamento
                'MUNICIPIO_DANE',         # Nombre del municipio según DANE
                'COD_DANE_MUNICIPIO'      # Código DANE del municipio
            )
            # Filtrar municipios que pertenecen a los departamentos seleccionados
            df_mun = df_mun.filter(df_mun['COD_DANE_DEPARTAMENTO'].isin(unidad_cod))

            # Convertir el DataFrame de Snowflake a un DataFrame de pandas
            data_mun_df = df_mun.to_pandas()

            if not data_mun_df.empty:
                # Extraer códigos y nombres únicos de los municipios
                municipio_turismo_cod = data_mun_df['COD_DANE_MUNICIPIO'].dropna().unique().tolist()
                municipio_turismo = data_mun_df['MUNICIPIO_DANE'].dropna().unique().tolist()
            else:
                # Si no hay datos, inicializar listas vacías
                municipio_turismo_cod = []
                municipio_turismo = []

            # Retornar el diccionario con los datos obtenidos
            return {
                'AGRUPACION': agrupacion,
                'UNIDAD': unidad,                            # Nombres de los departamentos
                'UNIDAD_COD': unidad_cod,                    # Códigos de los departamentos
                'MUNICIPIO_TURISMO_COD': municipio_turismo_cod,  # Códigos de municipios
                'MUNICIPIO_TURISMO': municipio_turismo,          # Nombres de municipios
                'UMBRAL': umbral
            }
        else:
            # Si no se encontraron departamentos, retornar valores por defecto
            return {
                'AGRUPACION': agrupacion,
                'UNIDAD': ['DEPARTAMENTO NO INCLUIDO'],
                'UNIDAD_COD': ['DEPARTAMENTO NO INCLUIDO'],
                'MUNICIPIO_TURISMO_COD': ['DEPARTAMENTO NO INCLUIDO'],
                'MUNICIPIO_TURISMO': ['DEPARTAMENTO NO INCLUIDO'],
                'UMBRAL': umbral
            }

    # Para las agrupaciones 'CONTINENTES', 'HUBS', 'TLCS', 'PAISES'
    elif agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:

        # Inicializar variables para almacenar resultados
        unidad = []
        paises_inversion = []
        paises_m49 = []
        paises_anexo_str = ''
        paises_cod_turismo = []
        paises_nombre_turismo = []
        nombre_pais = []

        # Caso 'CONTINENTES'
        if agrupacion == 'CONTINENTES':
            # Verifica que se haya proporcionado el parámetro 'continentes'
            if not continentes:
                raise ValueError("El parámetro 'continentes' es requerido cuando 'agrupacion' es 'CONTINENTES'")

            # Obtener datos de continentes desde la tabla en Snowflake
            df = session.table('DOCUMENTOS_COLOMBIA.GEOGRAFIA.CONTINENTES').select(
                'REGION_NAME',                    # Nombre de la región
                'REGION_NAME_EXPORTACIONES',      # Nombre de la región para exportaciones
                'REGION_NAME_TURISMO_AGREGADA'    # Nombre de la región para turismo
            )
            # Filtrar los continentes según la lista proporcionada
            df = df.filter(df['REGION_NAME'].isin(continentes))
            # Convertir el DataFrame de Snowflake a un DataFrame de pandas
            data_df = df.to_pandas()

            if not data_df.empty:
                # Extraer nombres únicos para exportaciones y turismo
                param_continente_exportaciones = data_df['REGION_NAME_EXPORTACIONES'].dropna().unique().tolist()
                param_continente_turismo = data_df['REGION_NAME_TURISMO_AGREGADA'].dropna().unique().tolist()
                unidad = param_continente_exportaciones  # La unidad será el nombre de la región para exportaciones
            else:
                # Si no hay datos, inicializar listas vacías
                param_continente_exportaciones = []
                param_continente_turismo = []
                unidad = []

        # Caso 'HUBS'
        elif agrupacion == 'HUBS':
            # Verifica que se haya proporcionado el parámetro 'hubs'
            if not hubs:
                raise ValueError("El parámetro 'hubs' es requerido cuando 'agrupacion' es 'HUBS'")

            # Obtener datos de hubs desde la tabla en Snowflake
            df = session.table('DOCUMENTOS_COLOMBIA.GEOGRAFIA.HUBS').select(
                'NOMBRE_HUB',                # Nombre del hub
                'HUB_NAME_EXPORTACIONES',    # Nombre del hub para exportaciones
                'HUB_NAME_TURISMO'           # Nombre del hub para turismo
            )
            # Filtrar los hubs según la lista proporcionada
            df = df.filter(df['NOMBRE_HUB'].isin(hubs))
            # Convertir el DataFrame de Snowflake a un DataFrame de pandas
            data_df = df.to_pandas()

            if not data_df.empty:
                # Extraer nombres únicos de hubs para exportaciones y turismo
                param_hub_exportaciones = data_df['HUB_NAME_EXPORTACIONES'].dropna().unique().tolist()
                param_hub_turismo = data_df['HUB_NAME_TURISMO'].dropna().unique().tolist()
                unidad = param_hub_exportaciones  # La unidad será el nombre del hub para exportaciones
            else:
                # Si no hay datos, inicializar listas vacías
                param_hub_exportaciones = []
                param_hub_turismo = []
                unidad = []

        # Caso 'PAISES'
        elif agrupacion == 'PAISES':
            # Verifica que se haya proporcionado el parámetro 'paises'
            if not paises:
                raise ValueError("El parámetro 'paises' es requerido cuando 'agrupacion' es 'PAISES'")

            # Obtener datos de países desde la tabla en Snowflake
            df = session.table('DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES').select(
                'COUNTRY_OR_AREA',           # Nombre real del país
                'PAIS_LLAVE_EXPORTACIONES'   # Nombre del país para exportaciones
            )
            # Filtrar los países según la lista proporcionada
            df = df.filter(df['COUNTRY_OR_AREA'].isin(paises))
            # Convertir el DataFrame de Snowflake a un DataFrame de pandas
            data_df = df.to_pandas()

            if not data_df.empty:
                # Extraer nombres únicos para exportaciones
                unidad = data_df['PAIS_LLAVE_EXPORTACIONES'].dropna().unique().tolist()
                nombre_pais = data_df['COUNTRY_OR_AREA'].dropna().unique().tolist()
            else:
                # Si no hay datos, inicializar lista vacía
                unidad = []

        # Caso 'TLCS'
        elif agrupacion == 'TLCS':
            # Verifica que se haya proporcionado el parámetro 'tlcs'
            if not tlcs:
                raise ValueError("El parámetro 'tlcs' es requerido cuando 'agrupacion' es 'TLCS'")

            # La unidad será la lista de TLCs proporcionada
            unidad = tlcs

        # Obtener datos de países para inversión
        df_inversion = session.table('DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES').select(
            'PAIS_INVERSION_BANREP',      # Nombre del país para inversión
            'COUNTRY_OR_AREA',            # Nombre real del país
            'M49_CODE',                   # Código M49 del país
            'REGION_NAME_EXPORTACIONES',  # Región para exportaciones
            'HUB_NAME_EXPORTACIONES',     # Hub para exportaciones
            'NOMBRE_TLC'                  # Nombre del TLC
        )

        # Aplicar filtros según los parámetros proporcionados y la agrupación
        if agrupacion == 'CONTINENTES':
            df_inversion = df_inversion.filter(df_inversion['REGION_NAME_EXPORTACIONES'].isin(param_continente_exportaciones))
        if agrupacion == 'HUBS':
            df_inversion = df_inversion.filter(df_inversion['HUB_NAME_EXPORTACIONES'].isin(param_hub_exportaciones))
        if agrupacion == 'PAISES':
            df_inversion = df_inversion.filter(df_inversion['COUNTRY_OR_AREA'].isin(paises))
        if agrupacion == 'TLCS':
            df_inversion = df_inversion.filter(df_inversion['NOMBRE_TLC'].isin(tlcs))

        # Convertir el DataFrame de Snowflake a un DataFrame de pandas
        data_inversion_df = df_inversion.to_pandas()

        if not data_inversion_df.empty:
            # Extraer listas únicas de países para inversión y códigos M49
            paises_inversion = data_inversion_df['PAIS_INVERSION_BANREP'].dropna().unique().tolist()
            paises_m49 = data_inversion_df['M49_CODE'].dropna().unique().tolist()
            paises_nombres = data_inversion_df['COUNTRY_OR_AREA'].dropna().unique()
            # Crear una cadena de nombres de países
            paises_anexo_str = ', '.join(paises_nombres)
        else:
            # Si no hay datos, inicializar listas vacías
            paises_inversion = []
            paises_m49 = []
            paises_anexo_str = ''

        # Obtener datos de países para turismo
        df_turismo = session.table('DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES').select(
            'CODIGO_PAIS_MIGRACION',      # Código del país para migración
            'NOMBRE_PAIS_MIGRACION',      # Nombre del país para migración
            'REGION_NAME_TURISMO_AGREGADA',  # Región para turismo
            'HUB_NAME_TURISMO',           # Hub para turismo
            'COUNTRY_OR_AREA',            # Nombre real del país
            'NOMBRE_TLC'                  # Nombre del TLC
        )

        # Aplicar filtros según los parámetros proporcionados y la agrupación
        if agrupacion == 'CONTINENTES':
            df_turismo = df_turismo.filter(df_turismo['REGION_NAME_TURISMO_AGREGADA'].isin(param_continente_turismo))
        if agrupacion == 'HUBS':
            df_turismo = df_turismo.filter(df_turismo['HUB_NAME_TURISMO'].isin(param_hub_turismo))
        if agrupacion == 'PAISES':
            df_turismo = df_turismo.filter(df_turismo['COUNTRY_OR_AREA'].isin(paises))
        if agrupacion == 'TLCS':
            df_turismo = df_turismo.filter(df_turismo['NOMBRE_TLC'].isin(tlcs))

        # Convertir el DataFrame de Snowflake a un DataFrame de pandas
        data_turismo_df = df_turismo.to_pandas()

        if not data_turismo_df.empty:
            # Extraer listas únicas de códigos y nombres de países para turismo
            paises_cod_turismo = data_turismo_df['CODIGO_PAIS_MIGRACION'].dropna().unique().tolist()
            paises_nombre_turismo = data_turismo_df['NOMBRE_PAIS_MIGRACION'].dropna().unique().tolist()
        else:
            # Si no hay datos, inicializar listas vacías
            paises_cod_turismo = []
            paises_nombre_turismo = []

        # Retornar el diccionario con todos los datos obtenidos
        if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS']:
            return {
                'AGRUPACION': agrupacion,
                'UNIDAD': unidad,                            # Nombres según la agrupación
                'PAISES_INVERSION': paises_inversion,        # Países para inversión
                'PAISES_TURISMO_COD': paises_cod_turismo,    # Códigos de países para turismo
                'PAISES_TURISMO': paises_nombre_turismo,     # Nombres de países para turismo
                'UMBRAL': umbral,
                'PAISES_ANEXO': paises_anexo_str,            # Nombres de países concatenados
                'M49_CODE': paises_m49                       # Códigos M49 de los países
            }
        elif agrupacion in ['PAISES']:
            return {
                'AGRUPACION': agrupacion,
                'UNIDAD': unidad,                            # Nombres según la agrupación
                'PAISES_INVERSION': paises_inversion,        # Países para inversión
                'PAISES_TURISMO_COD': paises_cod_turismo,    # Códigos de países para turismo
                'PAISES_TURISMO': paises_nombre_turismo,     # Nombres de países para turismo
                'UMBRAL': umbral,
                'PAISES_ANEXO': paises_anexo_str,            # Nombres de países concatenados
                'M49_CODE': paises_m49,                      # Códigos M49 de los países
                'NOMBRE PAIS' : nombre_pais                  # Nombres del país según ONU
            }

    else:
        # Si la agrupación no es reconocida, lanza un error
        raise ValueError(f"Agrupación '{agrupacion}' no reconocida. Opciones válidas: 'CONTINENTES', 'PAISES', 'HUBS', 'TLCS', 'DEPARTAMENTOS', 'COLOMBIA'.")

def verif_ejes(session, params):
    """
    Función para verificar la existencia de datos en diferentes categorías (exportaciones, inversión y turismo)
    agrupados por diferentes criterios (CONTINENTES, HUBS, TLCS, PAISES, DEPARTAMENTOS). La función ejecuta
    consultas SQL en una sesión de Snowflake y agrega los resultados a un diccionario indicando si hay datos
    disponibles o no en cada categoría y periodo (cerrado o corrido).

    Parámetros:
    - session: Sesión activa de Snowflake.
    - params: Diccionario con los parámetros necesarios para ejecutar las consultas, incluyendo AGRUPACION,
              UNIDAD, UMBRAL, PAISES_INVERSION, PAISES_TURISMO_COD, y UNIDAD_COD.

    Retorna:
    - dict_verif: Diccionario con los resultados de la verificación, indicando si hay datos disponibles o no
                  para cada categoría y periodo.
    """

    # 1. Obtener los parámetros según sea la agrupación y unidad
    AGRUPACION = params['AGRUPACION']
    UNIDAD = params['UNIDAD'][0]  # Tomamos el primer elemento de la lista

    # 2. Preparar las listas de países o departamentos según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        PAISES_INVERSION = [pais for pais in params.get('PAISES_INVERSION', []) if pais]
        PAISES_TURISMO_COD = [pais for pais in params.get('PAISES_TURISMO_COD', []) if pais]
        PAISES_INVERSION_sql = ', '.join(f"'{pais}'" for pais in PAISES_INVERSION)
        PAISES_TURISMO_sql = ', '.join(f"'{pais}'" for pais in PAISES_TURISMO_COD)
    elif AGRUPACION == 'DEPARTAMENTOS':
        DEPARTAMENTOS_TURISMO = [dep for dep in params.get('UNIDAD_COD', []) if dep]
        DEPARTAMENTOS_TURISMO_sql = ', '.join(f"'{dep}'" for dep in DEPARTAMENTOS_TURISMO)
    if AGRUPACION in ['CONTINENTES', 'PAISES', 'HUBS', 'TLCS']:
        PAISES_INVERSION_UNCTAD = [pais for pais in params.get('M49_CODE', []) if pais]
        PAISES_UNCTAD_sql = ', '.join(f"'{pais}'" for pais in PAISES_INVERSION_UNCTAD)


    # 3. Diccionario para almacenar los resultados
    dict_verif = {}

    # 4. Definir función auxiliar para verificar existencia de datos sin descargar todo el conjunto
    def data_exists(query):
        try:
            exists_query = f"SELECT 1 FROM ({query}) AS subquery LIMIT 1"
            result = session.sql(exists_query).collect()
            return bool(result)
        except Exception:
            return False

    # 5. Definir mapeo de cadenas para indicadores de datos
    indicadores_con_datos = {
        'exportaciones_totales_cerrado': 'CON DATOS DE EXPORTACIONES TOTALES CERRADO',
        'exportaciones_totales_corrido': 'CON DATOS DE EXPORTACIONES TOTALES CORRIDO',
        'exportaciones_nme_cerrado': 'CON DATOS DE EXPORTACIONES NME CERRADO',
        'exportaciones_nme_corrido': 'CON DATOS DE EXPORTACIONES NME CORRIDO',
        'ied_cerrado': 'CON DATOS DE IED CERRADO',
        'ied_corrido': 'CON DATOS DE IED CORRIDO',
        'ice_cerrado': 'CON DATOS DE ICE CERRADO',
        'ice_corrido': 'CON DATOS DE ICE CORRIDO',
        'turismo_cerrado': 'CON DATOS DE TURISMO CERRADO',
        'turismo_corrido': 'CON DATOS DE TURISMO CORRIDO',
        'exportaciones_conteo_cerrado': 'CON DATOS DE CONTEO CERRADO',
        'exportaciones_conteo_corrido': 'CON DATOS DE CONTEO CORRIDO',
        'exportaciones_empresas_cerrado': 'CON DATOS DE EMPRESAS CERRADO',
        'exportaciones_empresas_corrido': 'CON DATOS DE EMPRESAS CORRIDO',
        'oportunidades_exportacion': 'CON OPORTUNIDADES',
        'oportunidades_inversion': 'CON OPORTUNIDADES',
        'oportunidades_turismo': 'CON OPORTUNIDADES',
        'conectividad': 'CON DATOS DE CONECTIVIDAD',
        'pesos_minero_cerrado': 'CON DATOS CERRADO',
        'pesos_minero_corrido': 'CON DATOS CORRIDO',
        'pesos_no_minero_cerrado': 'CON DATOS CERRADO',
        'pesos_no_minero_corrido': 'CON DATOS CORRIDO',
        'ied_unctad': 'CON DATOS CERRADO',
        'ice_unctad': 'CON DATOS CERRADO',
        'balanza_comercial': 'CON DATOS CERRADO'
    }

    indicadores_sin_datos = {
        'exportaciones_totales_cerrado': 'SIN DATOS DE EXPORTACIONES TOTALES CERRADO',
        'exportaciones_totales_corrido': 'SIN DATOS DE EXPORTACIONES TOTALES CORRIDO',
        'exportaciones_nme_cerrado': 'SIN DATOS DE EXPORTACIONES NME CERRADO',
        'exportaciones_nme_corrido': 'SIN DATOS DE EXPORTACIONES NME CORRIDO',
        'ied_cerrado': 'SIN DATOS DE IED CERRADO',
        'ied_corrido': 'SIN DATOS DE IED CORRIDO',
        'ice_cerrado': 'SIN DATOS DE ICE CERRADO',
        'ice_corrido': 'SIN DATOS DE ICE CORRIDO',
        'turismo_cerrado': 'SIN DATOS DE TURISMO CERRADO',
        'turismo_corrido': 'SIN DATOS DE TURISMO CORRIDO',
        'exportaciones_conteo_cerrado': 'SIN DATOS DE CONTEO CERRADO',
        'exportaciones_conteo_corrido': 'SIN DATOS DE CONTEO CORRIDO',
        'exportaciones_empresas_cerrado': 'SIN DATOS DE EMPRESAS CERRADO',
        'exportaciones_empresas_corrido': 'SIN DATOS DE EMPRESAS CORRIDO',
        'oportunidades_exportacion': 'SIN OPORTUNIDADES',
        'oportunidades_inversion': 'SIN OPORTUNIDADES',
        'oportunidades_turismo': 'SIN OPORTUNIDADES',
        'conectividad': 'SIN DATOS DE CONECTIVIDAD',
        'pesos_minero_cerrado': 'SIN DATOS CERRADO',
        'pesos_minero_corrido': 'SIN DATOS CORRIDO',
        'pesos_no_minero_cerrado': 'SIN DATOS CERRADO',
        'pesos_no_minero_corrido': 'SIN DATOS CORRIDO',
        'ied_unctad': 'SIN DATOS CERRADO',
        'ice_unctad': 'SIN DATOS CERRADO',
        'balanza_comercial': 'SIN DATOS CERRADO'
    }

    # 6. El siguiente proceso no es válido para la agrupación de COLOMBIA ya que esta siempre tiene datos
    if AGRUPACION != 'COLOMBIA':

        # --------------------
        # Verificación de Exportaciones
        # --------------------

        # Lista de consultas para exportaciones
        export_queries = {
            'exportaciones_totales_cerrado': f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CERRADO
                WHERE AGRUPACION = '{AGRUPACION}' AND UNIDAD = '{UNIDAD}' AND TABLA = 'TOTAL' LIMIT 1
            """,
            'exportaciones_totales_corrido': f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CORRIDO
                WHERE AGRUPACION = '{AGRUPACION}' AND UNIDAD = '{UNIDAD}' AND TABLA = 'TOTAL' LIMIT 1
            """,
            'exportaciones_nme_cerrado': f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CERRADO
                WHERE AGRUPACION = '{AGRUPACION}' AND UNIDAD = '{UNIDAD}' AND TABLA = 'TIPOS' AND CATEGORIA = 'No Mineras' LIMIT 1
            """,
            'exportaciones_nme_corrido': f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CATEGORIAS_CORRIDO
                WHERE AGRUPACION = '{AGRUPACION}' AND UNIDAD = '{UNIDAD}' AND TABLA = 'TIPOS' AND CATEGORIA = 'No Mineras' LIMIT 1
            """,
            'exportaciones_conteo_cerrado': f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CONTEO_CERRADO
                WHERE AGRUPACION = '{AGRUPACION}' AND UNIDAD = '{UNIDAD}' LIMIT 1
            """,
            'exportaciones_conteo_corrido': f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_CONTEO_CORRIDO
                WHERE AGRUPACION = '{AGRUPACION}' AND UNIDAD = '{UNIDAD}' LIMIT 1
            """,
            'exportaciones_empresas_cerrado': f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_NIT_CERRADO
                WHERE AGRUPACION = '{AGRUPACION}' AND UNIDAD = '{UNIDAD}' LIMIT 1
            """,
            'exportaciones_empresas_corrido': f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.ST_NIT_CORRIDO
                WHERE AGRUPACION = '{AGRUPACION}' AND UNIDAD = '{UNIDAD}' LIMIT 1
            """
        }

        # Ejecutar consultas de exportaciones
        for key, query in export_queries.items():
            if key in indicadores_con_datos:
                dict_verif[key] = (
                    indicadores_con_datos[key]
                    if data_exists(query)
                    else indicadores_sin_datos[key]
                )

        # --------------------
        # Verificación de Inversión
        # --------------------
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES'] and PAISES_INVERSION:
            inversion_queries = {
                'ied_cerrado': f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO
                    WHERE AGRUPACION = 'PAISES' AND UNIDAD IN ({PAISES_INVERSION_sql})
                    AND CATEGORIA = 'IED' LIMIT 1
                """,
                'ied_corrido': f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO
                    WHERE AGRUPACION = 'PAISES' AND UNIDAD IN ({PAISES_INVERSION_sql})
                    AND CATEGORIA = 'IED' LIMIT 1
                """,
                'ice_cerrado': f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO
                    WHERE AGRUPACION = 'PAISES' AND UNIDAD IN ({PAISES_INVERSION_sql})
                    AND CATEGORIA = 'ICE' LIMIT 1
                """,
                'ice_corrido': f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CORRIDO
                    WHERE AGRUPACION = 'PAISES' AND UNIDAD IN ({PAISES_INVERSION_sql})
                    AND CATEGORIA = 'ICE' LIMIT 1
                """
            }

            # Ejecutar consultas de inversión
            for key, query in inversion_queries.items():
                if key in indicadores_con_datos:
                    dict_verif[key] = (
                        indicadores_con_datos[key]
                        if data_exists(query)
                        else indicadores_sin_datos[key]
                    )

        # ----------------------
        # Verificación de UNCTAD
        # ----------------------
        if AGRUPACION in ['CONTINENTES', 'PAISES', 'HUBS', 'TLCS'] and PAISES_UNCTAD_sql:
            unctad_queries = {
                'ied_unctad': f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.INVERSION.UNCTAD
                    WHERE ECONOMY IN ({PAISES_UNCTAD_sql})
                    AND DIRECTION_LABEL = 'IED' LIMIT 1
                """,
                'ice_unctad': f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.INVERSION.UNCTAD
                    WHERE ECONOMY IN ({PAISES_UNCTAD_sql})
                    AND DIRECTION_LABEL = 'ICE' LIMIT 1
                """
            }

            # Ejecutar consultas de unctad
            for key, query in unctad_queries.items():
                if key in indicadores_con_datos:
                    dict_verif[key] = (
                        indicadores_con_datos[key]
                        if data_exists(query)
                        else indicadores_sin_datos[key]
                    )

        # -----------------------
        # Verificación de Balanza
        # -----------------------
        if AGRUPACION in ['PAISES']: 
            balanza_queries = {
                'balanza_comercial': f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BALANZA
                    WHERE PAIS = '{UNIDAD}' LIMIT 1
                """
            }

            # Ejecutar consultas de unctad
            for key, query in balanza_queries.items():
                if key in indicadores_con_datos:
                    dict_verif[key] = (
                        indicadores_con_datos[key]
                        if data_exists(query)
                        else indicadores_sin_datos[key]
                    )   
        

        # --------------------
        # Verificación de Turismo
        # --------------------
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'DEPARTAMENTOS']:
            turismo_queries = {
                'turismo_cerrado': "SELECT 1 FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO WHERE 1=1",
                'turismo_corrido': "SELECT 1 FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO WHERE 1=1",
            }

            # Agregar condiciones según agrupación
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES'] and PAISES_TURISMO_COD:
                condition = f" AND PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql}) LIMIT 1"
                turismo_queries['turismo_cerrado'] += condition
                turismo_queries['turismo_corrido'] += condition
            elif AGRUPACION == 'DEPARTAMENTOS' and DEPARTAMENTOS_TURISMO:
                condition = f" AND DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql}) LIMIT 1"
                turismo_queries['turismo_cerrado'] += condition
                turismo_queries['turismo_corrido'] += condition
            else:
                turismo_queries['turismo_cerrado'] += " LIMIT 1"
                turismo_queries['turismo_corrido'] += " LIMIT 1"

            # Ejecutar consultas de turismo
            for key, query in turismo_queries.items():
                if key in indicadores_con_datos:
                    dict_verif[key] = (
                        indicadores_con_datos[key]
                        if data_exists(query)
                        else indicadores_sin_datos[key]
                    )

        # --------------------
        # Verificación de Conectividad (solo para DEPARTAMENTOS)
        # --------------------
        if AGRUPACION == 'DEPARTAMENTOS' and DEPARTAMENTOS_TURISMO and UNIDAD not in ['Bogotá']:
            query_conectividad = f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.TURISMO.CONECTIVIDAD
                WHERE COD_DIVIPOLA_DEPARTAMENTO_DESTINO IN ({DEPARTAMENTOS_TURISMO_sql}) LIMIT 1
            """

            if 'conectividad' in indicadores_con_datos and 'conectividad' in indicadores_sin_datos:
                dict_verif['conectividad'] = (
                    indicadores_con_datos['conectividad']
                    if data_exists(query_conectividad)
                    else indicadores_sin_datos['conectividad']
                )       
        
        # Crear consulta en caso de que sea Bogotá para capturar la conectividad de Cundinamarca
        if UNIDAD == 'Bogotá':
            query_conectividad = f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.TURISMO.CONECTIVIDAD
                WHERE COD_DIVIPOLA_DEPARTAMENTO_DESTINO IN ('25') LIMIT 1
            """      

            if 'conectividad' in indicadores_con_datos and 'conectividad' in indicadores_sin_datos:
                dict_verif['conectividad'] = (
                    indicadores_con_datos['conectividad']
                    if data_exists(query_conectividad)
                    else indicadores_sin_datos['conectividad']
                )

        # --------------------
        # Verificación de Oportunidades
        # --------------------
        oportunidades_types = {
            'oportunidades_exportacion': "Exportaciones",
            'oportunidades_inversion': "Inversión",
            'oportunidades_turismo': "Turismo",
        }

        for key, oportunidad in oportunidades_types.items():
            # Construir la consulta SQL según el tipo de oportunidad
            if key == 'oportunidades_exportacion':
                query_oportunidades = f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
                    WHERE A.EJE = '{oportunidad}'
                """
            elif key == 'oportunidades_inversion':
                query_oportunidades = f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
                    WHERE A.EJE = '{oportunidad}'
                """
            elif key == 'oportunidades_turismo':
                query_oportunidades = f"""
                    SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
                    WHERE A.EJE = '{oportunidad}'
                """
            else:
                continue  # Si hay otros tipos, los ignoramos por ahora

            # Agregar condiciones según la agrupación
            # Continentes
            if AGRUPACION == 'CONTINENTES':
                query_oportunidades += f" AND A.CONTINENTE IN ('{UNIDAD}')"
            # HUBS
            elif AGRUPACION == 'HUBS':
                query_oportunidades += f" AND A.HUB IN ('{UNIDAD}')"
            # PAISES
            elif AGRUPACION == 'PAISES':
                query_oportunidades += f" AND A.PAIS IN ('{UNIDAD}')"
            # TLCS
            elif AGRUPACION == 'TLCS':
                # Obtener países llave
                df_llave = pd.DataFrame(
                        session.sql(f"""
                            SELECT DISTINCT A.PAIS_LLAVE_EXPORTACIONES 
                            FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES AS A 
                            WHERE A.NOMBRE_TLC = '{UNIDAD}'
                        """).collect()
                    )
                # Obtener string de búsqueda de países
                PAISES = df_llave['PAIS_LLAVE_EXPORTACIONES'].dropna().unique().tolist()
                PAISES_sql = ', '.join(f"'{pais}'" for pais in PAISES)
                # Agregar filtro de tlcs
                query_oportunidades += f" AND A.PAIS IN ({PAISES_sql})"
                
            elif AGRUPACION == 'DEPARTAMENTOS' and DEPARTAMENTOS_TURISMO:
                query_oportunidades += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
            query_oportunidades += " LIMIT 1"

            # Verificar si existen indicadores definidos para la clave actual
            if key in indicadores_con_datos and key in indicadores_sin_datos:
                # Ejecutar consulta y asignar el indicador correspondiente
                dict_verif[key] = (
                    indicadores_con_datos[key]
                    if data_exists(query_oportunidades)
                    else indicadores_sin_datos[key]
                )


        # --------------------
        # Verificación de Pesos por Medio
        # --------------------
        pesos_types = {
            'pesos_minero_cerrado': ("MEDIO MINERAS", "ST_CATEGORIAS_PESO_CERRADO"),
            'pesos_minero_corrido': ("MEDIO MINERAS", "ST_CATEGORIAS_PESO_CORRIDO"),
            'pesos_no_minero_cerrado': ("MEDIO NO MINERAS", "ST_CATEGORIAS_PESO_CERRADO"),
            'pesos_no_minero_corrido': ("MEDIO NO MINERAS", "ST_CATEGORIAS_PESO_CORRIDO"),
        }

        for key, (tabla, dataset) in pesos_types.items():
            query_pesos = f"""
                SELECT 1 FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{dataset}
                WHERE TABLA = '{tabla}' AND AGRUPACION = '{AGRUPACION}' AND UNIDAD = '{UNIDAD}' LIMIT 1
            """
            if key in indicadores_con_datos and key in indicadores_sin_datos:
                dict_verif[key] = (
                    indicadores_con_datos[key]
                    if data_exists(query_pesos)
                    else indicadores_sin_datos[key]
                )

    else:
        # Si la agrupación es COLOMBIA, se asume que siempre hay datos
        default_keys = [
            'exportaciones_totales_cerrado', 'exportaciones_totales_corrido', 'exportaciones_nme_cerrado',
            'exportaciones_nme_corrido', 'exportaciones_conteo_cerrado', 'exportaciones_conteo_corrido',
            'exportaciones_empresas_cerrado', 'exportaciones_empresas_corrido', 'ied_cerrado', 'ied_corrido',
            'ice_cerrado', 'ice_corrido', 'turismo_cerrado', 'turismo_corrido', 'oportunidades_exportacion',
            'oportunidades_inversion', 'oportunidades_turismo', 'pesos_minero_cerrado',
            'pesos_minero_corrido', 'pesos_no_minero_cerrado', 'pesos_no_minero_corrido', 'ied_unctad', 'ice_unctad', 
            'balanza_comercial'
        ]
        for key in default_keys:
            if key in indicadores_con_datos:
                dict_verif[key] = indicadores_con_datos[key]
            elif key in indicadores_sin_datos:
                dict_verif[key] = indicadores_sin_datos[key]
        
        # No se muestran datos de conectividad para Colombia
        dict_verif['conectividad'] = 'SIN DATOS DE CONECTIVIDAD'

    return dict_verif



def calcular_diferencia_porcentual(valor_actual, valor_anterior):
    """
    Calcula la diferencia porcentual entre dos valores.

    La función sigue la lógica de cálculo condicional:
    - Si `valor_anterior` es 0 y `valor_actual` es mayor que 0, retorna 100.
    - Si `valor_anterior` es 0 y `valor_actual` es igual a 0, retorna 0.
    - Si `valor_actual` es 0 y `valor_anterior` es mayor que 0, retorna -100.
    - En cualquier otro caso, calcula y retorna la diferencia porcentual como:
      ((`valor_actual` - `valor_anterior`) / `valor_anterior`) * 100.

    Parámetros:
    - valor_actual (float): Valor en el periodo actual.
    - valor_anterior (float): Valor en el periodo anterior.

    Retorna:
    - float: La diferencia porcentual entre los dos periodos.
    """
    # Manejo de casos especiales cuando valor_anterior es cero
    if valor_anterior == 0:
        if valor_actual > 0:
            return 100
        elif valor_actual == 0:
            return 0
        else:  # valor_actual < 0
            return -100
    else:
        # Cálculo general de la diferencia porcentual
        return ((valor_actual - valor_anterior) / valor_anterior) * 100

def calcular_participacion_porcentual(df, columna, total, nombre_columna_resultante=None):
    """
    Calcula la participación porcentual de una columna específica en un DataFrame.

    Parámetros:
    df (pandas.DataFrame): El DataFrame que contiene los datos.
    columna (str): El nombre de la columna para la cual se calculará la participación porcentual.
    total (float): El total previamente calculado para usar como denominador.
    nombre_columna_resultante (str, opcional): El nombre de la columna resultante. Si no se especifica, 
                                               se usará el nombre original con el sufijo '_PARTICIPACION'.

    Retorna:
    pandas.DataFrame: El DataFrame con una nueva columna de participación porcentual.
    """
    if nombre_columna_resultante is None:
        nombre_columna_resultante = columna + '_PARTICIPACION'

    if total == 0:
        df[nombre_columna_resultante] = 0
    else:
        # Utilizar operaciones vectorizadas para mejorar el rendimiento
        df[nombre_columna_resultante] = np.where(
            df[columna] == 0,
            0,
            (df[columna] / total) * 100
        )

    return df



def get_data_exportaciones(session, geo_params, dict_verificacion):
    """
    Obtiene y procesa datos de exportaciones desde Snowflake, realizando cálculos adicionales y estructurando
    la información en diccionarios y DataFrames para su uso posterior.

    Parámetros:
    - session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    - geo_params (dict): Parámetros geográficos obtenidos de la función get_data_parametros().
    - dict_verificacion (dict): Diccionario de verificación obtenido de la función verif_ejes().

    Retorna:
    - dict: Un diccionario que contiene múltiples DataFrames y estructuras de datos con la información procesada.
    """

    # Obtener parámetros principales de geo_params
    AGRUPACION = geo_params['AGRUPACION']
    UNIDAD = geo_params['UNIDAD'][0]  # Tomamos el primer elemento de la lista

    # Inicializar diccionario para almacenar datos de resumen
    datos_resumen = {}

    # Definir las categorías y tablas a consultar
    if AGRUPACION in ['DEPARTAMENTOS', 'COLOMBIA']:
        categorias = ['CONTINENTE', 'DEPARTAMENTOS', 'PAIS', 'SECTORES', 'SUBSECTORES', 'TLCS']
    else:
        categorias = ['CONTINENTE', 'DEPARTAMENTOS', 'PAIS', 'SECTORES', 'SUBSECTORES']
    # Diccionarios que mapean nombres de tablas con claves de verificación
    tablas_usd = {
        'ST_CATEGORIAS_CERRADO': 'exportaciones_totales_cerrado',
        'ST_CATEGORIAS_CORRIDO': 'exportaciones_totales_corrido'
    }
    tablas_peso = {
        'ST_CATEGORIAS_PESO_CERRADO': 'exportaciones_totales_cerrado',
        'ST_CATEGORIAS_PESO_CORRIDO': 'exportaciones_totales_corrido'
    }
    tablas_nit_empresas = {
        'ST_NIT_CERRADO': 'exportaciones_empresas_cerrado',
        'ST_NIT_CORRIDO': 'exportaciones_empresas_corrido'
    }

    def ejecutar_consulta(tabla, verif_key, query_template, **kwargs):
        """
        Ejecuta una consulta SQL si hay datos disponibles según el diccionario de verificación.

        Parámetros:
        - tabla (str): Nombre de la tabla a consultar.
        - verif_key (str): Clave en dict_verificacion para verificar la disponibilidad de datos.
        - query_template (str): Plantilla de la consulta SQL con marcadores de posición.
        - **kwargs: Argumentos adicionales para formatear la plantilla de la consulta.

        Retorna:
        - pandas.DataFrame: Resultado de la consulta en forma de DataFrame.
        """
        # Verificar si hay datos disponibles
        if dict_verificacion.get(verif_key, '').startswith('CON DATOS'):
            # Formatear la consulta SQL con los parámetros adecuados, incluyendo argumentos adicionales
            query = query_template.format(tabla=tabla, AGRUPACION=AGRUPACION, UNIDAD=UNIDAD, **kwargs)
            # Ejecutar la consulta y convertir el resultado a DataFrame
            return pd.DataFrame(session.sql(query).collect())
        else:
            # Retornar un DataFrame vacío si no hay datos
            return pd.DataFrame()

    # =============================
    # 1. Totales de exportaciones en USD
    # =============================

    totales = {}
    for tabla, verif_key in tablas_usd.items():
        # Plantilla de consulta SQL para totales
        query_totales = """
            SELECT 'Total' AS CATEGORIA, 
                   A.SUMA_USD_T_1, 
                   A.SUMA_USD_T,  
                   A.DIFERENCIA_PORCENTUAL 
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
            WHERE A.AGRUPACION = '{AGRUPACION}' 
              AND A.UNIDAD = '{UNIDAD}'
              AND A.TABLA = 'TOTAL';
        """
        # Ejecutar la consulta y almacenar el resultado
        data = ejecutar_consulta(tabla, verif_key, query_totales)
        if not data.empty:
            totales[tabla] = data

    # =============================
    # 2. Tipos de exportaciones en USD
    # =============================

    tipos = {}
    for tabla, verif_key in tablas_usd.items():
        # Plantilla de consulta SQL para tipos
        query_tipos = """
            SELECT A.CATEGORIA, 
                   A.SUMA_USD_T_1, 
                   A.SUMA_USD_T, 
                   A.DIFERENCIA_PORCENTUAL 
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
            WHERE A.AGRUPACION = '{AGRUPACION}' 
              AND A.UNIDAD = '{UNIDAD}'
              AND A.TABLA = 'TIPOS';
        """
        # Ejecutar la consulta y almacenar el resultado
        data = ejecutar_consulta(tabla, verif_key, query_tipos)
        if not data.empty and tabla in totales:
            # Calcular el total para participación
            total_t = totales[tabla]['SUMA_USD_T'].sum()
            # Combinar datos de tipos con totales
            data = pd.concat([data, totales[tabla]], ignore_index=True)
            # Calcular participación porcentual
            data = calcular_participacion_porcentual(data, 'SUMA_USD_T', total_t, 'PARTICIPACION_T')
            tipos[tabla] = data

            # Agregar datos al resumen
            datos_resumen[tabla] = {}
            for _, row in data.iterrows():
                categoria = row['CATEGORIA']
                datos_resumen[tabla].setdefault(categoria, []).append({
                    'sum_usd_t_1': row['SUMA_USD_T_1'],
                    'sum_usd_t': row['SUMA_USD_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                    'participacion_t': row['PARTICIPACION_T']
                })

    # =============================
    # 3. Datos por categoría (años cerrado y corrido)
    # =============================

    categorias_data = {'CERRADO': {}, 'CORRIDO': {}}

    # Definir periodos y sus correspondientes tablas y claves de verificación
    periodos = [
        ('CERRADO', 'ST_CATEGORIAS_CERRADO', 'exportaciones_nme_cerrado'),
        ('CORRIDO', 'ST_CATEGORIAS_CORRIDO', 'exportaciones_nme_corrido')
    ]

    for periodo, tabla_usd, verif_key in periodos:
        if dict_verificacion.get(verif_key, '').startswith('CON DATOS'):
            for categoria in categorias:
                # Plantilla de consulta SQL para categorías
                query_categoria = """
                    SELECT A.CATEGORIA, 
                           A.SUMA_USD_T_1, 
                           A.SUMA_USD_T, 
                           A.DIFERENCIA_PORCENTUAL 
                    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
                    WHERE A.AGRUPACION = '{AGRUPACION}' 
                      AND A.UNIDAD = '{UNIDAD}'
                      AND A.TABLA = '{categoria}'
                    ORDER BY A.SUMA_USD_T DESC;
                """
                # Ejecutar la consulta y almacenar el resultado
                data = ejecutar_consulta(
                    tabla_usd, 
                    verif_key, 
                    query_categoria,
                    categoria=categoria  # Pasar 'categoria' como argumento adicional
                )
                if not data.empty:
                    # Procesar los datos obtenidos
                    row_num = data.shape[0]
                    data_top = data.head(5)  # Tomar los primeros 5 registros

                    # Obtener datos totales de 'No Mineras' para calcular participaciones
                    df_totales_nme = tipos[tabla_usd]
                    df_totales_nme = df_totales_nme[df_totales_nme['CATEGORIA'] == 'No Mineras'].copy()
                    df_totales_nme['CATEGORIA'] = 'Total'

                    total_t = df_totales_nme['SUMA_USD_T'].sum()
                    total_t_1 = df_totales_nme['SUMA_USD_T_1'].sum()

                    # Calcular datos para la categoría 'Otros'
                    otros_t = total_t - data_top['SUMA_USD_T'].sum()
                    otros_t_1 = total_t_1 - data_top['SUMA_USD_T_1'].sum()
                    otros_porcentual = calcular_diferencia_porcentual(otros_t, otros_t_1)

                    # Crear DataFrame para 'Otros'
                    otros_df = pd.DataFrame({
                        'CATEGORIA': ['Otros'],
                        'SUMA_USD_T_1': [otros_t_1],
                        'SUMA_USD_T': [otros_t],
                        'DIFERENCIA_PORCENTUAL': [otros_porcentual]
                    })

                    # Caso especial para TLCS
                    if categoria in ['TLCS'] and row_num <= 5 and (otros_t != 0 or otros_t_1 != 0):
                        # Modificar row num para agregar otros
                        row_num = 6

                    # Combinar datos dependiendo del número de registros
                    if row_num <= 5:
                        data_final = pd.concat([data_top, df_totales_nme], ignore_index=True)
                    else:
                        data_final = pd.concat([data_top, otros_df, df_totales_nme], ignore_index=True)

                    # Calcular participación porcentual
                    data_final = calcular_participacion_porcentual(data_final, 'SUMA_USD_T', total_t, 'PARTICIPACION_T')

                    # Almacenar los datos procesados
                    categorias_data[periodo][categoria] = data_final

    # =============================
    # 4. Información de empresas
    # =============================

    empresas = {}
    for tabla, verif_key in tablas_nit_empresas.items():
        # Plantilla de consulta SQL para empresas
        query_empresas = """
            WITH CTE AS (
                SELECT A.CATEGORIA, 
                    A.RAZON_SOCIAL,
                    A.SECTOR_ESTRELLA,
                    A.SUMA_USD_T_1, 
                    A.SUMA_USD_T, 
                    A.DIFERENCIA_PORCENTUAL,
                    ROW_NUMBER() OVER (PARTITION BY A.CATEGORIA ORDER BY A.CATEGORIA) AS RN
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A 
                WHERE A.AGRUPACION = '{AGRUPACION}' 
                    AND A.UNIDAD = '{UNIDAD}'
            )
            SELECT CATEGORIA, 
                RAZON_SOCIAL,
                SECTOR_ESTRELLA,
                SUMA_USD_T_1, 
                SUMA_USD_T, 
                DIFERENCIA_PORCENTUAL
            FROM CTE
            WHERE RN = 1
            ORDER BY SUMA_USD_T DESC;
        """
        # Ejecutar la consulta y almacenar el resultado
        data = ejecutar_consulta(tabla, verif_key, query_empresas)
        row_num_inicial = data.shape[0]
        if not data.empty:
            # Procesar los datos obtenidos
            row_num = data.shape[0]
            data_top = data.head(5)  # Tomar los primeros 5 registros para asegurar la existencia de otros
            # Lógica para agregar los casos en que hay -1
            if 'NO DEFINIDO' in data['RAZON_SOCIAL'].unique():
                row_num = row_num - 1
                data_top = data_top[data_top['RAZON_SOCIAL'] != 'NO DEFINIDO']

            # Obtener datos totales de 'No Mineras' para calcular participaciones
            df_totales_nme = tipos.get(tabla.replace('ST_NIT', 'ST_CATEGORIAS'), pd.DataFrame())
            df_totales_nme = df_totales_nme[df_totales_nme['CATEGORIA'] == 'No Mineras'].copy()
            df_totales_nme['CATEGORIA'] = 'Total'
            df_totales_nme['RAZON_SOCIAL'] = 'No aplica'
            df_totales_nme['SECTOR_ESTRELLA'] = 'No aplica'

            total_t = df_totales_nme['SUMA_USD_T'].sum()
            total_t_1 = df_totales_nme['SUMA_USD_T_1'].sum()

            # Calcular datos para la categoría 'Otros'
            otros_t = total_t - data_top['SUMA_USD_T'].sum()
            otros_t_1 = total_t_1 - data_top['SUMA_USD_T_1'].sum()
            otros_porcentual = calcular_diferencia_porcentual(otros_t, otros_t_1)

            # Crear DataFrame para 'Otros'
            otros_df = pd.DataFrame({
                'CATEGORIA': ['Otros'],
                'RAZON_SOCIAL': ['No aplica'],
                'SECTOR_ESTRELLA': ['No aplica'],
                'SUMA_USD_T_1': [otros_t_1],
                'SUMA_USD_T': [otros_t],
                'DIFERENCIA_PORCENTUAL': [otros_porcentual]
            })

            # Combinar datos dependiendo del número de registros
            if row_num == 5:
                data_final = pd.concat([data_top, df_totales_nme], ignore_index=True)
            if row_num < 5:
                data_final = pd.concat([data_top, otros_df, df_totales_nme], ignore_index=True)
            if row_num > 5:
                data_final = pd.concat([data_top, otros_df, df_totales_nme], ignore_index=True)
            # Otro condicional para asegurar que 
            if row_num_inicial == 6 and 'NO DEFINIDO' in data['RAZON_SOCIAL'].unique():
                data_final = pd.concat([data_top, otros_df, df_totales_nme], ignore_index=True)  

            # Calcular participación porcentual
            data_final = calcular_participacion_porcentual(data_final, 'SUMA_USD_T', total_t, 'PARTICIPACION_T')

            # Almacenar los datos procesados
            empresas[tabla] = data_final

    # =============================
    # 5. Conteo de empresas únicas por año
    # =============================

    conteo = {}
    # Diccionario para mapeo de periodos y sus claves
    periodos_conteo = [
        ('CERRADO', 'ST_CONTEO_CERRADO', 'exportaciones_conteo_cerrado'),
        ('CORRIDO', 'ST_CONTEO_CORRIDO', 'exportaciones_conteo_corrido')
    ]

    # Inicializar entrada en datos_resumen para conteo
    datos_resumen['CONTEO'] = {}

    for periodo, tabla_conteo, verif_key in periodos_conteo:
        if dict_verificacion.get(verif_key, '').startswith('CON DATOS'):
            # Plantilla de consulta SQL para conteo
            query_conteo = """
                SELECT CAST(A.CONTEO_T AS INT) AS CONTEO_T
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
                WHERE A.AGRUPACION = '{AGRUPACION}' 
                  AND A.UNIDAD = '{UNIDAD}';
            """
            # Ejecutar la consulta y almacenar el resultado
            data = ejecutar_consulta(tabla_conteo, verif_key, query_conteo)
            if not data.empty:
                conteo[periodo] = data['CONTEO_T'].iloc[0]
                # Agregar datos al resumen
                datos_resumen['CONTEO'][periodo] = data['CONTEO_T'].iloc[0]

    # =============================
    # 6. Totales y tipos de exportaciones en peso
    # =============================

    totales_peso = {}
    tipos_peso = {}
    for tabla, verif_key in tablas_peso.items():
        # Plantilla de consulta SQL para totales en peso
        query_totales_peso = """
            SELECT 'Total' AS CATEGORIA, 
                   A.SUMA_PESO_T_1, 
                   A.SUMA_PESO_T, 
                   A.DIFERENCIA_PORCENTUAL 
            FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
            WHERE A.AGRUPACION = '{AGRUPACION}'
              AND A.UNIDAD = '{UNIDAD}'
              AND A.TABLA = 'TOTAL';
        """
        # Ejecutar la consulta y almacenar el resultado
        data_totales = ejecutar_consulta(tabla, verif_key, query_totales_peso)
        if not data_totales.empty:
            totales_peso[tabla] = data_totales

            # Plantilla de consulta SQL para tipos en peso
            query_tipos_peso = """
                SELECT A.CATEGORIA, 
                       A.SUMA_PESO_T_1, 
                       A.SUMA_PESO_T, 
                       A.DIFERENCIA_PORCENTUAL 
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
                WHERE A.AGRUPACION = '{AGRUPACION}'
                  AND A.UNIDAD = '{UNIDAD}'
                  AND A.TABLA = 'TIPOS';
            """
            # Ejecutar la consulta y almacenar el resultado
            data_tipos = ejecutar_consulta(tabla, verif_key, query_tipos_peso)
            if not data_tipos.empty:
                # Calcular el total para participación
                total_t = data_totales['SUMA_PESO_T'].sum()
                # Combinar datos de tipos con totales
                data_tipos = pd.concat([data_tipos, data_totales], ignore_index=True)
                # Calcular participación porcentual
                data_tipos = calcular_participacion_porcentual(data_tipos, 'SUMA_PESO_T', total_t, 'PARTICIPACION_T')
                tipos_peso[tabla] = data_tipos

                # Agregar datos al resumen
                datos_resumen[tabla] = {}
                for _, row in data_tipos.iterrows():
                    categoria = row['CATEGORIA']
                    datos_resumen[tabla].setdefault(categoria, []).append({
                        'sum_peso_t_1': row['SUMA_PESO_T_1'],
                        'sum_peso_t': row['SUMA_PESO_T'],
                        'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                        'participacion_t': row['PARTICIPACION_T']
                    })

    # =============================
    # 7. Pesos por medio de transporte (Mineras y No Mineras)
    # =============================

    medios_peso = {'MINERO': {}, 'NO_MINERO': {}}

    # Definir tipos de medios y sus correspondientes claves de verificación
    medios_transporte = [
        ('MINERO', 'MEDIO MINERAS', 'pesos_minero_cerrado', 'pesos_minero_corrido'),
        ('NO_MINERO', 'MEDIO NO MINERAS', 'pesos_no_minero_cerrado', 'pesos_no_minero_corrido')
    ]

    for tipo_medio, tabla_medio, verif_key_cerrado, verif_key_corrido in medios_transporte:
        periodos_peso = [
            ('ST_CATEGORIAS_PESO_CERRADO', verif_key_cerrado),
            ('ST_CATEGORIAS_PESO_CORRIDO', verif_key_corrido)
        ]
        for tabla, verif_key in periodos_peso:
            if dict_verificacion.get(verif_key, '').startswith('CON DATOS'):
                # Plantilla de consulta SQL para medios de transporte
                query_medios_peso = """
                    SELECT A.CATEGORIA, 
                           A.SUMA_PESO_T_1, 
                           A.SUMA_PESO_T,
                           A.DIFERENCIA_PORCENTUAL
                    FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.{tabla} AS A
                    WHERE A.AGRUPACION = '{AGRUPACION}'
                      AND A.UNIDAD = '{UNIDAD}'
                      AND A.TABLA = '{tabla_medio}';
                """
                # Ejecutar la consulta y almacenar el resultado
                data = ejecutar_consulta(
                    tabla, 
                    verif_key, 
                    query_medios_peso,
                    tabla_medio=tabla_medio  # Pasar 'tabla_medio' como argumento adicional
                )
                if not data.empty:
                    # Calcular totales
                    total_t_1 = data['SUMA_PESO_T_1'].sum()
                    total_t = data['SUMA_PESO_T'].sum()
                    diferencia_porcentual_t = calcular_diferencia_porcentual(total_t, total_t_1)
                    # Crear DataFrame para totales
                    total_df = pd.DataFrame({
                        'CATEGORIA': ['Total'],
                        'SUMA_PESO_T_1': [total_t_1],
                        'SUMA_PESO_T': [total_t],
                        'DIFERENCIA_PORCENTUAL' : [diferencia_porcentual_t]
                    })
                    # Combinar datos con totales
                    data = pd.concat([data, total_df], ignore_index=True)
                    # Calcular participación porcentual
                    data = calcular_participacion_porcentual(data, 'SUMA_PESO_T', total_t, 'PARTICIPACION_T')
                    # Almacenar los datos procesados
                    medios_peso[tipo_medio][tabla] = data
    
    # ===========
    # 7.5 Balanza
    # ===========

    balanza={}
    tablas_balanza = {'BALANZA':'balanza_comercial'}

    for tabla, verif_key in tablas_balanza.items():
        # Plantilla de consulta SQL
        query_balanza = """ 
            SELECT *
            FROM (
                SELECT A.YEAR, A.TIPO, CAST(A.BALANZA AS BIGINT) AS BALANZA
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.BALANZA AS A
                WHERE A.PAIS = '{UNIDAD}'
            ) AS SourceTable
            PIVOT (
                SUM(BALANZA)
                FOR YEAR IN (2020, 2021, 2022, 2023) 
            ) AS PivotTable;
        """
        # Ejecutar la consulta y almacenar el resultado
        data = ejecutar_consulta(tabla, verif_key, query_balanza)
        if not data.empty:
            balanza[tabla] = data

    # =============================
    # 8. Retornar todos los resultados en un diccionario
    # =============================

    return {
        'TOTALES': totales,
        'TIPOS': tipos,
        'CATEGORIAS CERRADO': categorias_data['CERRADO'],
        'CATEGORIAS CORRIDO': categorias_data['CORRIDO'],
        'EMPRESAS': empresas,
        'CONTEO EMPRESAS': conteo,
        'TOTALES PESO': totales_peso,
        'TIPOS PESO': tipos_peso,
        'RESUMEN': datos_resumen,
        'MEDIOS PESO MINERO': medios_peso['MINERO'],
        'MEDIOS PESO NO MINERO': medios_peso['NO_MINERO'],
        'BALANZA': balanza
    }



def get_data_inversion(session, geo_params, dict_verificacion):
    """
    Obtiene y procesa datos de inversión (IED e ICE) desde Snowflake, realizando cálculos adicionales y estructurando
    la información en diccionarios y DataFrames para su uso posterior.

    Parámetros:
    - session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    - geo_params (dict): Parámetros geográficos obtenidos de la función get_data_parametros().
    - dict_verificacion (dict): Diccionario de verificación obtenido de la función verif_ejes().

    Retorna:
    - dict: Un diccionario que contiene múltiples DataFrames y estructuras de datos con la información procesada.
    """

    # Obtener el parámetro AGRUPACION, UNIDAD y NOMBRE de geo_params
    AGRUPACION = geo_params['AGRUPACION']
    NOMBRE_UNIDAD = geo_params['UNIDAD'][0]
    if AGRUPACION in ['PAISES']:
        NOMBRE_UNIDAD_PAIS = geo_params['NOMBRE PAIS'][0]

    # Preparar la lista de países de inversión si corresponde
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        PAISES_INVERSION = [pais for pais in geo_params.get('PAISES_INVERSION', []) if pais is not None]
        PAISES_INVERSION_sql = ', '.join(f"'{pais}'" for pais in PAISES_INVERSION)
    else:
        PAISES_INVERSION = []
        PAISES_INVERSION_sql = ''

    # Lista de países para UNCTAD
    if AGRUPACION in ['CONTINENTES', 'PAISES', 'HUBS', 'TLCS']:
        PAISES_INVERSION_UNCTAD = [pais for pais in geo_params.get('M49_CODE', []) if pais]
        PAISES_UNCTAD_sql = ', '.join(f"'{pais}'" for pais in PAISES_INVERSION_UNCTAD)

    # Inicializar diccionarios para almacenar los resultados
    datos_resumen = {}
    ied_colombia_actividades = {}
    ied_paises = {}
    ice_paises = {}
    ied_total = {}
    ice_total = {}
    unctad = {}

    # =============================
    # Verificación para Agrupación "DEPARTAMENTOS"
    # =============================
    if AGRUPACION == 'DEPARTAMENTOS':
        # Retornar diccionarios vacíos sin ejecutar ninguna lógica adicional
        return {
            'IED ACTIVIDADES COLOMBIA': {},
            'IED PAISES': {},
            'IED TOTAL': {},
            'ICE PAISES': {},
            'ICE TOTAL': {},
            'RESUMEN': {}
        }

    def ejecutar_consulta(query, verif_key):
        """
        Ejecuta una consulta SQL si hay datos disponibles según el diccionario de verificación.

        Parámetros:
        - query (str): La consulta SQL a ejecutar.
        - verif_key (str): Clave en dict_verificacion para verificar la disponibilidad de datos.

        Retorna:
        - pandas.DataFrame: Resultado de la consulta en forma de DataFrame.
        """
        if dict_verificacion.get(verif_key, '').startswith('CON DATOS'):
            try:
                # Ejecutar la consulta y convertir el resultado a DataFrame
                return pd.DataFrame(session.sql(query).collect())
            except Exception as e:
                print(f"Error ejecutando la consulta para {verif_key}: {e}")
                return pd.DataFrame()
        else:
            # Retornar un DataFrame vacío si no hay datos
            return pd.DataFrame()

    # =============================
    # 1. IED ACTIVIDADES COLOMBIA
    # =============================

    if AGRUPACION == 'COLOMBIA':
        # Definir consultas para año cerrado y corrido
        query_ied_actividades_cerrado = f"""
        SELECT A.UNIDAD,
               A.SUMA_INVERSION_T_1,
               A.SUMA_INVERSION_T,
               A.DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_ACTIVIDADES_CERRADO AS A
        WHERE A.AGRUPACION = 'ACTIVIDADES'
          AND A.UNIDAD NOT IN ('TOTAL')
          AND A.UNIDAD IN ('Servicios financieros y empresariales',
                           'Industrias manufactureras',
                           'Comercio al por mayor y al por menor, restaurantes y hoteles',
                           'Transportes, almacenamiento y comunicaciones',
                           'Electricidad, gas y agua',
                           'Servicios comunales sociales y personales',
                           'Construcción',
                           'Agricultura, caza, silvicultura y pesca')
          AND A.TABLA = 'INVERSIÓN ACTIVIDADES'
          AND A.CATEGORIA = 'IED';
        """

        query_ied_actividades_corrido = f"""
        SELECT A.UNIDAD,
               A.SUMA_INVERSION_T_1,
               A.SUMA_INVERSION_T,
               A.DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_ACTIVIDADES_CORRIDO AS A
        WHERE A.AGRUPACION = 'ACTIVIDADES'
          AND A.UNIDAD NOT IN ('TOTAL')
          AND A.UNIDAD IN ('Servicios financieros y empresariales',
                           'Industrias manufactureras',
                           'Comercio al por mayor y al por menor, restaurantes y hoteles',
                           'Transportes, almacenamiento y comunicaciones',
                           'Electricidad, gas y agua',
                           'Servicios comunales sociales y personales',
                           'Construcción',
                           'Agricultura, caza, silvicultura y pesca')
          AND A.TABLA = 'INVERSIÓN ACTIVIDADES'
          AND A.CATEGORIA = 'IED';
        """

        # Ejecutar consultas para año cerrado y corrido
        ied_actividades_cerrado = ejecutar_consulta(query_ied_actividades_cerrado, 'ied_cerrado')
        ied_actividades_corrido = ejecutar_consulta(query_ied_actividades_corrido, 'ied_corrido')

        # Procesar datos para año cerrado si hay datos
        if not ied_actividades_cerrado.empty:
            # Calcular totales
            total_t_1 = ied_actividades_cerrado['SUMA_INVERSION_T_1'].sum()
            total_t = ied_actividades_cerrado['SUMA_INVERSION_T'].sum()
            diferencia_porcentual_total = calcular_diferencia_porcentual(total_t, total_t_1)

            # Crear fila de totales
            total_row = pd.DataFrame({
                'UNIDAD': ['Total'],
                'SUMA_INVERSION_T_1': [total_t_1],
                'SUMA_INVERSION_T': [total_t],
                'DIFERENCIA_PORCENTUAL_T': [diferencia_porcentual_total]
            })

            # Concatenar los datos con la fila de totales
            ied_actividades_cerrado = pd.concat([ied_actividades_cerrado, total_row], ignore_index=True)

            # Calcular participación porcentual
            ied_actividades_cerrado = calcular_participacion_porcentual(
                ied_actividades_cerrado, 'SUMA_INVERSION_T', total_t, 'PARTICIPACION_T'
            )

            # Almacenar los resultados
            ied_colombia_actividades['ied_cerrado'] = ied_actividades_cerrado

            # Agregar al resumen
            datos_resumen['IED CERRADO ACTIVIDADES'] = {}
            for _, row in ied_actividades_cerrado.iterrows():
                unidad = row['UNIDAD']
                datos_resumen['IED CERRADO ACTIVIDADES'].setdefault(unidad, []).append({
                    'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                    'sum_inversion_t': row['SUMA_INVERSION_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T'],
                    'participacion_t': row['PARTICIPACION_T']
                })

        # Procesar datos para año corrido si hay datos
        if not ied_actividades_corrido.empty:
            # Calcular totales
            total_t_1 = ied_actividades_corrido['SUMA_INVERSION_T_1'].sum()
            total_t = ied_actividades_corrido['SUMA_INVERSION_T'].sum()
            diferencia_porcentual_total = calcular_diferencia_porcentual(total_t, total_t_1)

            # Crear fila de totales
            total_row = pd.DataFrame({
                'UNIDAD': ['Total'],
                'SUMA_INVERSION_T_1': [total_t_1],
                'SUMA_INVERSION_T': [total_t],
                'DIFERENCIA_PORCENTUAL': [diferencia_porcentual_total]
            })

            # Concatenar los datos con la fila de totales
            ied_actividades_corrido = pd.concat([ied_actividades_corrido, total_row], ignore_index=True)

            # Calcular participación porcentual
            ied_actividades_corrido = calcular_participacion_porcentual(
                ied_actividades_corrido, 'SUMA_INVERSION_T', total_t, 'PARTICIPACION_T'
            )

            # Almacenar los resultados
            ied_colombia_actividades['ied_corrido'] = ied_actividades_corrido

            # Agregar al resumen
            datos_resumen['IED CORRIDO ACTIVIDADES'] = {}
            for _, row in ied_actividades_corrido.iterrows():
                unidad = row['UNIDAD']
                datos_resumen['IED CORRIDO ACTIVIDADES'].setdefault(unidad, []).append({
                    'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                    'sum_inversion_t': row['SUMA_INVERSION_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                    'participacion_t': row['PARTICIPACION_T']
                })

    # =============================
    # 2. IED POR PAÍSES
    # =============================

    # Procesar datos de IED por países para agrupaciones relevantes
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        # Definir periodos y sus respectivas claves de verificación
        periodos = [
            ('cerrado', 'DIFERENCIA_PORCENTUAL_T', 'ied_cerrado', 'ied_cerrado'),  # Asegurar verif_key correcta
            ('corrido', 'DIFERENCIA_PORCENTUAL', 'ied_corrido', 'ied_corrido')   # Asegurar verif_key correcta
        ]

        for periodo_name, diferencia_col, dict_key, verif_key in periodos:
            # Construir consulta para países
            query_paises_ied = f"""
            SELECT A.UNIDAD,
                   A.SUMA_INVERSION_T_1,
                   A.SUMA_INVERSION_T,
                   A.{diferencia_col} AS DIFERENCIA_PORCENTUAL
            FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_{periodo_name.upper()} AS A
            WHERE A.AGRUPACION = 'PAISES'
              AND A.UNIDAD NOT IN ('TOTAL')
              AND A.CATEGORIA = 'IED'
            """

            # Añadir condición para países de inversión si aplica
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES'] and PAISES_INVERSION_sql:
                query_paises_ied += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"

            query_paises_ied += " ORDER BY A.SUMA_INVERSION_T DESC;"

            # Ejecutar consulta
            ied_paises_df = ejecutar_consulta(query_paises_ied, verif_key)

            if not ied_paises_df.empty:
                # Procesar datos
                row_num = ied_paises_df.shape[0]
                if AGRUPACION in ['COLOMBIA']:
                    ied_paises_top = ied_paises_df.head(10)
                else:
                    ied_paises_top = ied_paises_df.head(5)

                # Construir consulta para totales
                if AGRUPACION == 'COLOMBIA':
                    # Para COLOMBIA, obtener el total de IED
                    query_total = f"""
                    SELECT A.UNIDAD,
                           A.SUMA_INVERSION_T_1,
                           A.SUMA_INVERSION_T,
                           A.{diferencia_col} AS DIFERENCIA_PORCENTUAL
                    FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_{periodo_name.upper()} AS A
                    WHERE A.AGRUPACION = 'PAISES'
                      AND A.UNIDAD = 'TOTAL'
                      AND A.CATEGORIA = 'IED';
                    """
                else:
                    # Para otras agrupaciones, sumar las inversiones
                    query_total = f"""
                    SELECT 'TOTAL' AS UNIDAD,
                           SUM(A.SUMA_INVERSION_T_1) AS SUMA_INVERSION_T_1,
                           SUM(A.SUMA_INVERSION_T) AS SUMA_INVERSION_T,
                           CASE 
                               WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) > 0 THEN 100
                               WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) = 0 THEN 0
                               WHEN SUM(A.SUMA_INVERSION_T) = 0 AND SUM(A.SUMA_INVERSION_T_1) > 0 THEN -100
                               ELSE ((SUM(A.SUMA_INVERSION_T) - SUM(A.SUMA_INVERSION_T_1)) / SUM(A.SUMA_INVERSION_T_1)) * 100
                           END AS DIFERENCIA_PORCENTUAL
                    FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_{periodo_name.upper()} AS A
                    WHERE A.AGRUPACION = 'PAISES'
                      AND A.CATEGORIA = 'IED'
                    """
                    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS'] and PAISES_INVERSION_sql:
                        query_total += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql});"
                    else:
                        query_total += ";"

                # Ejecutar consulta para totales
                total_df = ejecutar_consulta(query_total, verif_key)
                
                # Calcular 'Otros' si corresponde
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                    otros_t_1 = total_df['SUMA_INVERSION_T_1'].sum() - ied_paises_top['SUMA_INVERSION_T_1'].sum()
                    otros_t = total_df['SUMA_INVERSION_T'].sum() - ied_paises_top['SUMA_INVERSION_T'].sum()
                    diferencia_porcentual_otros = calcular_diferencia_porcentual(otros_t, otros_t_1)

                    otros_df = pd.DataFrame({
                        'UNIDAD': ['Otros'],
                        'SUMA_INVERSION_T_1': [otros_t_1],
                        'SUMA_INVERSION_T': [otros_t],
                        'DIFERENCIA_PORCENTUAL': [diferencia_porcentual_otros]
                    })

                    # Concatenar los datos
                    if row_num <= 5:
                        ied_paises_final = pd.concat([ied_paises_top, total_df], ignore_index=True)
                    else:
                        ied_paises_final = pd.concat([ied_paises_top, otros_df, total_df], ignore_index=True)

                    # Calcular participación
                    total_inversion_t = total_df['SUMA_INVERSION_T'].sum()
                    ied_paises_final = calcular_participacion_porcentual(
                        ied_paises_final, 'SUMA_INVERSION_T', total_inversion_t, 'PARTICIPACION_T'
                    )
                else:
                    ied_paises_final = ied_paises_top

                # Almacenar los resultados
                ied_paises[dict_key] = ied_paises_final

                # Agregar al resumen
                resumen_key = f"IED {periodo_name.upper()} PAISES"
                datos_resumen[resumen_key] = {}
                for _, row in ied_paises_final.iterrows():
                    unidad = row['UNIDAD']
                    entry = {
                        'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                        'sum_inversion_t': row['SUMA_INVERSION_T'],
                        'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL']
                    }
                    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                        entry['participacion_t'] = row['PARTICIPACION_T']
                    datos_resumen[resumen_key].setdefault(unidad, []).append(entry)

                # Procesar datos totales para agrupaciones
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                    # Obtener total IED mundial
                    query_total_mundo = f"""
                    SELECT 'Total IED del Mundo en Colombia' AS UNIDAD,
                           A.SUMA_INVERSION_T_1,
                           A.SUMA_INVERSION_T,
                           A.{diferencia_col} AS DIFERENCIA_PORCENTUAL
                    FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_{periodo_name.upper()} AS A
                    WHERE A.AGRUPACION = 'PAISES'
                      AND A.UNIDAD = 'TOTAL'
                      AND A.CATEGORIA = 'IED';
                    """
                    total_mundo_df = ejecutar_consulta(query_total_mundo, verif_key)

                    # Combinar datos para cálculo de participación
                    if AGRUPACION == 'PAISES':
                        agrupaciones_df = pd.concat([ied_paises_top, total_mundo_df], ignore_index=True)
                    else:
                        agrupaciones_df = pd.concat([total_df, total_mundo_df], ignore_index=True)

                    total_mundo_t = total_mundo_df['SUMA_INVERSION_T'].sum()
                    agrupaciones_df = calcular_participacion_porcentual(
                        agrupaciones_df, 'SUMA_INVERSION_T', total_mundo_t, 'PARTICIPACION_T'
                    )

                    # Almacenar los resultados
                    ied_total[f"{dict_key}_total"] = agrupaciones_df

                    # Agregar al resumen
                    resumen_total_key = f"IED {periodo_name.upper()} TOTAL"
                    datos_resumen[resumen_total_key] = {}
                    for _, row in agrupaciones_df.iterrows():
                        unidad = row['UNIDAD']
                        entry = {
                            'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                            'sum_inversion_t': row['SUMA_INVERSION_T'],
                            'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                            'participacion_t': row['PARTICIPACION_T']
                        }
                        datos_resumen[resumen_total_key].setdefault(unidad, []).append(entry)

    # =============================
    # 2.5 IED POR PAÍSES ACUMULADO
    # =============================
    
    # Procesar datos de IED para mensaje acumulado
    if (AGRUPACION in ['PAISES']) and (dict_verificacion['ied_cerrado'] == 'CON DATOS DE IED CERRADO'):
        # Construir query acumulada
        query_ied_acumulado = """
        SELECT A.UNIDAD, A.VALOR, A.RANKING
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO_ACUMULADO AS A
        WHERE A.TIPO = 'IED'
        """ 
        # Agregar filtro
        if PAISES_INVERSION_sql:
            query_ied_acumulado += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql});"
        
        # Ejecutar consulta
        df_ied_acumulado = pd.DataFrame(session.sql(query_ied_acumulado).collect())

        # Extraer los datos para el diccionario de resumen 
        resumen_key = "IED PAISES ACUMULADA"
        datos_resumen[resumen_key] = {}
        for _, row in df_ied_acumulado.iterrows():
            unidad = 'PAIS'
            entry = {
                'sum_inversion_acumulada': row['VALOR'],
                'ranking': row['RANKING']
            }
            datos_resumen[resumen_key].setdefault(unidad, []).append(entry)
            
    
    # =============================
    # 3. ICE POR PAÍSES
    # =============================

    # Procesar datos de ICE por países de manera similar al bloque anterior
    # Definir periodos y sus respectivas claves de verificación para ICE
    periodos_ice = [
        ('cerrado', 'DIFERENCIA_PORCENTUAL_T', 'ice_cerrado', 'ice_cerrado'),  # Asegurar verif_key correcta
        ('corrido', 'DIFERENCIA_PORCENTUAL', 'ice_corrido', 'ice_corrido')   # Asegurar verif_key correcta
    ]

    for periodo_name, diferencia_col, dict_key, verif_key in periodos_ice:
        # Construir consulta para países
        query_paises_ice = f"""
        SELECT A.UNIDAD,
               A.SUMA_INVERSION_T_1,
               A.SUMA_INVERSION_T,
               A.{diferencia_col} AS DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_{periodo_name.upper()} AS A
        WHERE A.AGRUPACION = 'PAISES'
          AND A.UNIDAD NOT IN ('TOTAL')
          AND A.CATEGORIA = 'ICE'
        """

        # Añadir condición para países de inversión si aplica
        if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES'] and PAISES_INVERSION_sql:
            query_paises_ice += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql})"

        query_paises_ice += " ORDER BY A.SUMA_INVERSION_T DESC;"

        # Ejecutar consulta
        ice_paises_df = ejecutar_consulta(query_paises_ice, verif_key)

        if not ice_paises_df.empty:
            # Procesar datos
            row_num = ice_paises_df.shape[0]
            ice_paises_top = ice_paises_df.head(5)

            # Construir consulta para totales
            if AGRUPACION == 'COLOMBIA':
                # Para COLOMBIA, obtener el total de ICE
                query_total = f"""
                SELECT A.UNIDAD,
                       A.SUMA_INVERSION_T_1,
                       A.SUMA_INVERSION_T,
                       A.{diferencia_col} AS DIFERENCIA_PORCENTUAL
                FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_{periodo_name.upper()} AS A
                WHERE A.AGRUPACION = 'PAISES'
                  AND A.UNIDAD = 'TOTAL'
                  AND A.CATEGORIA = 'ICE';
                """
            else:
                # Para otras agrupaciones, sumar las inversiones
                query_total = f"""
                SELECT 'TOTAL' AS UNIDAD,
                       SUM(A.SUMA_INVERSION_T_1) AS SUMA_INVERSION_T_1,
                       SUM(A.SUMA_INVERSION_T) AS SUMA_INVERSION_T,
                       CASE 
                           WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) > 0 THEN 100
                           WHEN SUM(A.SUMA_INVERSION_T_1) = 0 AND SUM(A.SUMA_INVERSION_T) = 0 THEN 0
                           WHEN SUM(A.SUMA_INVERSION_T) = 0 AND SUM(A.SUMA_INVERSION_T_1) > 0 THEN -100
                           ELSE ((SUM(A.SUMA_INVERSION_T) - SUM(A.SUMA_INVERSION_T_1)) / SUM(A.SUMA_INVERSION_T_1)) * 100
                       END AS DIFERENCIA_PORCENTUAL
                FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_{periodo_name.upper()} AS A
                WHERE A.AGRUPACION = 'PAISES'
                  AND A.CATEGORIA = 'ICE'
                """
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS'] and PAISES_INVERSION_sql:
                    query_total += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql});"
                else:
                    query_total += ";"

            # Ejecutar consulta para totales
            total_df = ejecutar_consulta(query_total, verif_key)

            # Calcular 'Otros' si corresponde
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                otros_t_1 = total_df['SUMA_INVERSION_T_1'].sum() - ice_paises_top['SUMA_INVERSION_T_1'].sum()
                otros_t = total_df['SUMA_INVERSION_T'].sum() - ice_paises_top['SUMA_INVERSION_T'].sum()
                diferencia_porcentual_otros = calcular_diferencia_porcentual(otros_t, otros_t_1)

                otros_df = pd.DataFrame({
                    'UNIDAD': ['Otros'],
                    'SUMA_INVERSION_T_1': [otros_t_1],
                    'SUMA_INVERSION_T': [otros_t],
                    'DIFERENCIA_PORCENTUAL': [diferencia_porcentual_otros]
                })

                # Concatenar los datos
                if row_num <= 5:
                    ice_paises_final = pd.concat([ice_paises_top, total_df], ignore_index=True)
                else:
                    ice_paises_final = pd.concat([ice_paises_top, otros_df, total_df], ignore_index=True)

                # Calcular participación
                total_inversion_t = total_df['SUMA_INVERSION_T'].sum()
                ice_paises_final = calcular_participacion_porcentual(
                    ice_paises_final, 'SUMA_INVERSION_T', total_inversion_t, 'PARTICIPACION_T'
                )
            else:
                ice_paises_final = ice_paises_top

            # Almacenar los resultados
            ice_paises[dict_key] = ice_paises_final

            # Agregar al resumen
            resumen_key = f"ICE {periodo_name.upper()} PAISES"
            datos_resumen[resumen_key] = {}
            for _, row in ice_paises_final.iterrows():
                unidad = row['UNIDAD']
                entry = {
                    'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                    'sum_inversion_t': row['SUMA_INVERSION_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL']
                }
                if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'COLOMBIA']:
                    entry['participacion_t'] = row['PARTICIPACION_T']
                datos_resumen[resumen_key].setdefault(unidad, []).append(entry)

            # Procesar datos totales para agrupaciones
            if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
                # Obtener total ICE mundial
                query_total_mundo = f"""
                SELECT 'Total ICE de Colombia en el Mundo' AS UNIDAD,
                       A.SUMA_INVERSION_T_1,
                       A.SUMA_INVERSION_T,
                       A.{diferencia_col} AS DIFERENCIA_PORCENTUAL
                FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_{periodo_name.upper()} AS A
                WHERE A.AGRUPACION = 'PAISES'
                  AND A.UNIDAD = 'TOTAL'
                  AND A.CATEGORIA = 'ICE';
                """
                total_mundo_df = ejecutar_consulta(query_total_mundo, verif_key)

                # Combinar datos para cálculo de participación
                if AGRUPACION == 'PAISES':
                    agrupaciones_df = pd.concat([ice_paises_top, total_mundo_df], ignore_index=True)
                else:
                    agrupaciones_df = pd.concat([total_df, total_mundo_df], ignore_index=True)

                total_mundo_t = total_mundo_df['SUMA_INVERSION_T'].sum()
                agrupaciones_df = calcular_participacion_porcentual(
                    agrupaciones_df, 'SUMA_INVERSION_T', total_mundo_t, 'PARTICIPACION_T'
                )

                # Almacenar los resultados
                ice_total[f"{dict_key}_total"] = agrupaciones_df

                # Agregar al resumen
                resumen_total_key = f"ICE {periodo_name.upper()} TOTAL"
                datos_resumen[resumen_total_key] = {}
                for _, row in agrupaciones_df.iterrows():
                    unidad = row['UNIDAD']
                    entry = {
                        'sum_inversion_t_1': row['SUMA_INVERSION_T_1'],
                        'sum_inversion_t': row['SUMA_INVERSION_T'],
                        'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                        'participacion_t': row['PARTICIPACION_T']
                    }
                    datos_resumen[resumen_total_key].setdefault(unidad, []).append(entry)

    # Agregar nombres de agrupación en agregados
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS']:
        # Verificamos la existencia y no vaciedad de cada DataFrame antes de intentar modificar la columna 'UNIDAD'
        
        # Modificar la primera fila de 'ied_cerrado_total' si la clave existe y el DataFrame no está vacío
        if 'ied_cerrado_total' in ied_total and not ied_total['ied_cerrado_total'].empty:
            # Verificamos que la columna 'UNIDAD' exista antes de modificarla
            if 'UNIDAD' in ied_total['ied_cerrado_total'].columns:
                # Cambiar solo la primera fila de la columna 'UNIDAD'
                ied_total['ied_cerrado_total'].loc[0, 'UNIDAD'] = f"Total IED de {NOMBRE_UNIDAD} en Colombia"
        
        # Modificar la primera fila de 'ied_corrido_total' de manera similar
        if 'ied_corrido_total' in ied_total and not ied_total['ied_corrido_total'].empty:
            if 'UNIDAD' in ied_total['ied_corrido_total'].columns:
                ied_total['ied_corrido_total'].loc[0, 'UNIDAD'] = f"Total IED de {NOMBRE_UNIDAD} en Colombia"
        
        # Modificar la primera fila de 'ice_cerrado_total'
        if 'ice_cerrado_total' in ice_total and not ice_total['ice_cerrado_total'].empty:
            if 'UNIDAD' in ice_total['ice_cerrado_total'].columns:
                ice_total['ice_cerrado_total'].loc[0, 'UNIDAD'] = f"Total ICE de Colombia en {NOMBRE_UNIDAD}"
        
        # Modificar la primera fila de 'ice_corrido_total'
        if 'ice_corrido_total' in ice_total and not ice_total['ice_corrido_total'].empty:
            if 'UNIDAD' in ice_total['ice_corrido_total'].columns:
                ice_total['ice_corrido_total'].loc[0, 'UNIDAD'] = f"Total ICE de Colombia en {NOMBRE_UNIDAD}"

    # Agregar nombre limpio para países
    if AGRUPACION in ['PAISES']:
        # Modificar la primera fila de 'ied_cerrado_total' si la clave existe y el DataFrame no está vacío
        if 'ied_cerrado_total' in ied_total and not ied_total['ied_cerrado_total'].empty:
            # Verificar que la columna 'UNIDAD' exista
            if 'UNIDAD' in ied_total['ied_cerrado_total'].columns:
                # Cambiar solo la primera fila de la columna 'UNIDAD'
                ied_total['ied_cerrado_total'].loc[0, 'UNIDAD'] = f"{NOMBRE_UNIDAD_PAIS}"
        
        # Modificar la primera fila de 'ied_corrido_total' de manera similar
        if 'ied_corrido_total' in ied_total and not ied_total['ied_corrido_total'].empty:
            if 'UNIDAD' in ied_total['ied_corrido_total'].columns:
                ied_total['ied_corrido_total'].loc[0, 'UNIDAD'] = f"{NOMBRE_UNIDAD_PAIS}"

        # Modificar la primera fila de 'ice_cerrado_total'
        if 'ice_cerrado_total' in ice_total and not ice_total['ice_cerrado_total'].empty:
            if 'UNIDAD' in ice_total['ice_cerrado_total'].columns:
                ice_total['ice_cerrado_total'].loc[0, 'UNIDAD'] = f"{NOMBRE_UNIDAD_PAIS}"
        
        # Modificar la primera fila de 'ice_corrido_total'
        if 'ice_corrido_total' in ice_total and not ice_total['ice_corrido_total'].empty:
            if 'UNIDAD' in ice_total['ice_corrido_total'].columns:
                ice_total['ice_corrido_total'].loc[0, 'UNIDAD'] = f"{NOMBRE_UNIDAD_PAIS}"

    # =============================
    # 3.5 ICE POR PAÍSES ACUMULADO
    # =============================
    
    # Procesar datos de ICE para mensaje acumulado
    if (AGRUPACION in ['PAISES']) and (dict_verificacion['ice_cerrado'] == 'CON DATOS DE ICE CERRADO'):
        # Construir query acumulada
        query_ice_acumulado = """
        SELECT A.UNIDAD, A.VALOR, A.RANKING
        FROM DOCUMENTOS_COLOMBIA.INVERSION.ST_PAISES_CERRADO_ACUMULADO AS A
        WHERE A.TIPO = 'ICE'
        """ 
        # Agregar filtro
        if PAISES_INVERSION_sql:
            query_ice_acumulado += f" AND A.UNIDAD IN ({PAISES_INVERSION_sql});"
        
        # Ejecutar consulta
        df_ice_acumulado = pd.DataFrame(session.sql(query_ice_acumulado).collect())

        # Extraer los datos para el diccionario de resumen 
        resumen_key = "ICE PAISES ACUMULADA"
        datos_resumen[resumen_key] = {}
        for _, row in df_ice_acumulado.iterrows():
            unidad = 'PAIS'
            entry = {
                'sum_inversion_acumulada': row['VALOR'],
                'ranking': row['RANKING']
            }
            datos_resumen[resumen_key].setdefault(unidad, []).append(entry)
        
    # ============
    # 3.5. UNCTAD
    # ============
    # Continentes y países
    if AGRUPACION in ['CONTINENTES', 'PAISES', 'HUBS', 'TLCS']:
        periodos_unctad = [
            ('IED', 'ied_unctad'),
            ('ICE', 'ice_unctad')
        ]
        # Procesar datos de UNCTAD
        for direction, dict_key in periodos_unctad: 
            # Crear query para IED e ICE
            query_unctad = f"""
                    SELECT *
            FROM (
                SELECT A.ECONOMY, A.YEAR, CAST(A.US_CURRENT_PRICES_MILLIONS AS BIGINT) AS Valor
                FROM DOCUMENTOS_COLOMBIA.INVERSION.UNCTAD AS A
                WHERE A.YEAR IN ('2018', '2019', '2020', '2021', '2022', '2023')
                    AND A.DIRECTION_LABEL = '{direction}'
                    AND A.ECONOMY IN ({PAISES_UNCTAD_sql})
            ) AS SourceTable
            PIVOT (
                MAX(Valor) 
                FOR YEAR IN (2018, 2019, 2020, 2021, 2022, 2023)
            ) AS PivotTable;
                    """
            # Ejecutar consulta
            inversion_unctad_df = ejecutar_consulta(query_unctad, dict_key)

            # Llenar valores vacíos con 0
            inversion_unctad_df = inversion_unctad_df.fillna(0)

            # Verificar si el DataFrame está vacío
            if inversion_unctad_df.empty:
                continue  # Saltar esta iteración si el DataFrame está vacío
              
            # Agregar datos al resumen
            # Llave
            resumen_key = dict_key
            datos_resumen[resumen_key] = {}

            if AGRUPACION in ['PAISES']:
                # Extraer información y agregar al diccionario
                for _, row in inversion_unctad_df.iterrows():
                    # Extraer datos de la tabla en estas agrupaciones porque no se debe sumar
                    entry = {
                            'fdi_2022': row['2022'],
                            'fdi_2023': row['2023']
                        }
                    datos_resumen[resumen_key] = entry
            elif AGRUPACION in ['HUBS', 'TLCS', 'CONTINENTES']:
                # Extraer datos de la tabla en estas agrupaciones porque se debe sumar
                fdi_2022 = inversion_unctad_df['2022'].sum()
                fdi_2023 = inversion_unctad_df['2023'].sum()
                entry = {
                            'fdi_2022': fdi_2022,
                            'fdi_2023': fdi_2023
                        }
                datos_resumen[resumen_key] = entry
            
            # Agregar total para HUBS y TLCs y CONTINENTES
            if AGRUPACION in ['HUBS', 'TLCS', 'CONTINENTES']:
                # Sumas de año
                sum_2018 = inversion_unctad_df['2018'].sum()
                sum_2019 = inversion_unctad_df['2019'].sum()
                sum_2020 = inversion_unctad_df['2020'].sum()
                sum_2021 = inversion_unctad_df['2021'].sum()
                sum_2022 = inversion_unctad_df['2022'].sum()
                sum_2023 = inversion_unctad_df['2023'].sum()
                sum_economy = 'Total'
                # Crear dataframe
                total_df = pd.DataFrame({
                    'ECONOMY': [sum_economy],
                    '2018': [sum_2018], 
                    '2019': [sum_2019], 
                    '2020': [sum_2020], 
                    '2021': [sum_2021], 
                    '2022': [sum_2022], 
                    '2023': [sum_2023]
    
                })
                # Concatenar
                inversion_unctad_df = pd.concat([inversion_unctad_df, total_df])

            # Agregar datos
            unctad[dict_key] = inversion_unctad_df  

    # Colombia
    if AGRUPACION == 'COLOMBIA':
        periodos_unctad = [
            ('IED', 'ied_unctad'),
            ('ICE', 'ice_unctad')
        ]
        # Procesar datos de UNCTAD
        for direction, dict_key in periodos_unctad: 
            # Crear query para IED e ICE
            query_unctad = f"""
                    SELECT *
            FROM (
                SELECT A.ECONOMY, A.YEAR, CAST(A.US_CURRENT_PRICES_MILLIONS AS BIGINT) AS Valor
                FROM DOCUMENTOS_COLOMBIA.INVERSION.UNCTAD AS A
                WHERE A.YEAR IN ('2018', '2019', '2020', '2021', '2022', '2023')
                    AND A.DIRECTION_LABEL = '{direction}'
                    AND A.ECONOMY IN ('32',	'68',	'76',	'152',	'170',	'188',	'218',	'484',	'591',	'600',	'604',	'214',	'858',	'862')
            ) AS SourceTable
            PIVOT (
                MAX(Valor) 
                FOR YEAR IN (2018, 2019, 2020, 2021, 2022, 2023)
            ) AS PivotTable;
                    """
            # Ejecutar consulta
            inversion_unctad_df = ejecutar_consulta(query_unctad, dict_key)

            # Almacenar resultados
            unctad[dict_key] = inversion_unctad_df


    # =============================
    # 4. Retornar todos los resultados
    # =============================

    return {
        'IED ACTIVIDADES COLOMBIA': ied_colombia_actividades,
        'IED PAISES': ied_paises,
        'IED TOTAL': ied_total,
        'ICE PAISES': ice_paises,
        'ICE TOTAL': ice_total,
        'UNCTAD' : unctad,
        'RESUMEN': datos_resumen
    }


def get_data_turismo(session, geo_params, dict_verificacion):
    """
    Obtiene y procesa datos de turismo desde Snowflake, basándose en los parámetros geográficos especificados.
    La función recupera datos tanto para los años 'CERRADO' como 'CORRIDO', realiza cálculos adicionales
    y estructura la información en diccionarios y DataFrames para su uso posterior.

    Parámetros:
    - session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    - geo_params (dict): Parámetros geográficos obtenidos de la función get_data_parametros().
    - dict_verificacion (dict): Diccionario de verificación obtenido de la función verif_ejes().

    Retorna:
    - dict: Un diccionario que contiene múltiples DataFrames y estructuras de datos con la información procesada.
    """

    ######################################
    # OBTENER PARÁMETROS SEGÚN SEA EL CASO
    ######################################

    # Extraer el parámetro 'AGRUPACION' de geo_params
    AGRUPACION = geo_params['AGRUPACION']
    
    # Preparar los parámetros para los datos de turismo según la agrupación
    # Si la agrupación es por países, continentes, hubs o TLCs
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        # Obtener la lista de códigos de países de turismo
        PAISES_TURISMO = [pais for pais in geo_params['PAISES_TURISMO_COD'] if pais is not None]
        # Formatear la lista para usarla en las consultas SQL
        PAISES_TURISMO_sql = ', '.join(f"'{pais}'" for pais in PAISES_TURISMO)
    # Si la agrupación es por departamentos
    if AGRUPACION in ['DEPARTAMENTOS']:
        # Obtener la lista de códigos de departamentos de turismo
        DEPARTAMENTOS_TURISMO = [departamento for departamento in geo_params['UNIDAD_COD'] if departamento is not None]
        # Formatear la lista para usarla en las consultas SQL
        DEPARTAMENTOS_TURISMO_sql = ', '.join(f"'{departamento}'" for departamento in DEPARTAMENTOS_TURISMO)
    
    ##################################
    # Diccionario para hoja de resumen
    ##################################
    datos_resumen = {}
    
    #########
    # TURISMO
    #########

    # Diccionarios para almacenar los resultados
    turismo_cerrado = {}   # Datos para el año 'CERRADO'
    turismo_corrido = {}   # Datos para el año 'CORRIDO'

    #########
    # CERRADO
    #########

    # Construir consulta para obtener datos de turismo por países en el año 'CERRADO'
    query_paises_turismo_paises_cerrado = f"""
        SELECT A.PAIS_RESIDENCIA,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
        WHERE 1=1
    """
    # Agregar condiciones a la consulta según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_paises_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_paises_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_paises_cerrado += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_paises_cerrado += f" GROUP BY A.PAIS_RESIDENCIA ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Construir consultas similares para departamentos, municipios, género y motivo en el año 'CERRADO'
    # Consulta para departamentos
    query_paises_turismo_departamentos_cerrado = f"""
        SELECT A.DPTO_HOSPEDAJE,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
        WHERE 1=1
    """
    # Agregar condiciones según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_departamentos_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_departamentos_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_departamentos_cerrado += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_departamentos_cerrado += f" GROUP BY A.DPTO_HOSPEDAJE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Consulta para municipios
    query_paises_turismo_municipio_cerrado = f"""
        SELECT A.CIUDAD_HOSPEDAJE,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
        WHERE 1=1
    """
    # Agregar condiciones según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_municipio_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_municipio_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_municipio_cerrado += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_municipio_cerrado += f" GROUP BY A.CIUDAD_HOSPEDAJE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Consulta para género
    query_paises_turismo_genero_cerrado = f"""
        SELECT A.DESCRIPCION_GENERO,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
        WHERE 1=1
    """
    # Agregar condiciones según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_genero_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_genero_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_genero_cerrado += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_genero_cerrado += f" GROUP BY A.DESCRIPCION_GENERO ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Consulta para motivo de viaje
    query_paises_turismo_motivo_cerrado = f"""
        SELECT A.MOVC_NOMBRE,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL_T
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CERRADO AS A
        WHERE 1=1
    """
    # Agregar condiciones según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_motivo_cerrado += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_motivo_cerrado += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_motivo_cerrado += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_motivo_cerrado += f" GROUP BY A.MOVC_NOMBRE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    #########
    # CORRIDO
    #########

    # Construir consultas similares para el año 'CORRIDO'
    # Consulta para países
    query_paises_turismo_paises_corrido = f"""
        SELECT A.PAIS_RESIDENCIA,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO AS A
        WHERE 1=1
    """
    # Agregar condiciones según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_paises_corrido += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_paises_corrido += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_paises_corrido += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_paises_corrido += f" GROUP BY A.PAIS_RESIDENCIA ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Consulta para departamentos
    query_paises_turismo_departamentos_corrido = f"""
        SELECT A.DPTO_HOSPEDAJE,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO AS A
        WHERE 1=1
    """
    # Agregar condiciones según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_departamentos_corrido += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_departamentos_corrido += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_departamentos_corrido += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_departamentos_corrido += f" GROUP BY A.DPTO_HOSPEDAJE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Consulta para municipios
    query_paises_turismo_municipio_corrido = f"""
        SELECT A.CIUDAD_HOSPEDAJE,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO AS A
        WHERE 1=1
    """
    # Agregar condiciones según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_municipio_corrido += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_municipio_corrido += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_municipio_corrido += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_municipio_corrido += f" GROUP BY A.CIUDAD_HOSPEDAJE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Consulta para género
    query_paises_turismo_genero_corrido = f"""
        SELECT A.DESCRIPCION_GENERO,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO AS A
        WHERE 1=1
    """
    # Agregar condiciones según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_genero_corrido += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_genero_corrido += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_genero_corrido += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_genero_corrido += f" GROUP BY A.DESCRIPCION_GENERO ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    # Consulta para motivo de viaje
    query_paises_turismo_motivo_corrido = f"""
        SELECT A.MOVC_NOMBRE,
            SUM(A.SUMA_TURISMO_T_1) AS SUMA_TURISMO_T_1,
            SUM(A.SUMA_TURISMO_T) AS SUMA_TURISMO_T,
            CASE 
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) > 0 THEN 100
                WHEN SUM(A.SUMA_TURISMO_T_1) = 0 AND SUM(A.SUMA_TURISMO_T) = 0 THEN 0
                WHEN SUM(A.SUMA_TURISMO_T) = 0 AND SUM(A.SUMA_TURISMO_T_1) > 0 THEN -100
                ELSE ((SUM(A.SUMA_TURISMO_T) - SUM(A.SUMA_TURISMO_T_1)) / SUM(A.SUMA_TURISMO_T_1)) * 100
            END AS DIFERENCIA_PORCENTUAL
        FROM DOCUMENTOS_COLOMBIA.TURISMO.ST_PAISES_CORRIDO AS A
        WHERE 1=1
    """
    # Agregar condiciones según la agrupación
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        query_paises_turismo_motivo_corrido += f" AND A.PAIS_RESIDENCIA IN ({PAISES_TURISMO_sql})"
    if AGRUPACION in ['DEPARTAMENTOS']:
        query_paises_turismo_motivo_corrido += f" AND A.DPTO_HOSPEDAJE IN ({DEPARTAMENTOS_TURISMO_sql})"
    if AGRUPACION == 'COLOMBIA':
        query_paises_turismo_motivo_corrido += f" AND 1=1"
    # Agrupar y ordenar los resultados
    query_paises_turismo_motivo_corrido += f" GROUP BY A.MOVC_NOMBRE ORDER BY SUM(A.SUMA_TURISMO_T) DESC;"

    ######################################
    # EJECUTAR CONSULTAS Y PROCESAR DATOS
    ######################################

    # Ejecutar consultas y procesar datos para el año 'CERRADO' si hay datos disponibles
    if dict_verificacion['turismo_cerrado'] == 'CON DATOS DE TURISMO CERRADO':
        # Ejecutar las consultas y almacenar los resultados en DataFrames de pandas
        turismo_paises_cerrado = pd.DataFrame(session.sql(query_paises_turismo_paises_cerrado).collect())
        turismo_departamentos_cerrado = pd.DataFrame(session.sql(query_paises_turismo_departamentos_cerrado).collect())
        turismo_municipio_cerrado = pd.DataFrame(session.sql(query_paises_turismo_municipio_cerrado).collect())
        turismo_genero_cerrado = pd.DataFrame(session.sql(query_paises_turismo_genero_cerrado).collect())
        turismo_motivo_cerrado = pd.DataFrame(session.sql(query_paises_turismo_motivo_cerrado).collect())

        # Calcular el número de filas en cada DataFrame para determinar si se debe agregar la categoría 'Otros'
        row_num_turismo_paises_cerrado = turismo_paises_cerrado.shape[0]
        row_num_turismo_departamentos_cerrado = turismo_departamentos_cerrado.shape[0]
        row_num_turismo_municipio_cerrado = turismo_municipio_cerrado.shape[0]
        row_num_turismo_genero_cerrado = turismo_genero_cerrado.shape[0]
        row_num_turismo_motivo_cerrado = turismo_motivo_cerrado.shape[0]

        # Calcular los totales generales
        turismo_total_cerrado_t_1 = turismo_paises_cerrado['SUMA_TURISMO_T_1'].sum()
        turismo_total_cerrado_t = turismo_paises_cerrado['SUMA_TURISMO_T'].sum()
        turismo_total_cerrado_diferencia_porcentual = calcular_diferencia_porcentual(turismo_total_cerrado_t, turismo_total_cerrado_t_1)

        # Tomar los top 5 registros en cada DataFrame
        turismo_paises_cerrado = turismo_paises_cerrado.head(5)
        turismo_departamentos_cerrado = turismo_departamentos_cerrado.head(5)
        turismo_municipio_cerrado = turismo_municipio_cerrado.head(5)
        turismo_motivo_cerrado = turismo_motivo_cerrado.head(5)

        # Crear listas de DataFrames y sus tamaños originales
        dataframes = [
            (turismo_paises_cerrado, row_num_turismo_paises_cerrado),
            (turismo_departamentos_cerrado, row_num_turismo_departamentos_cerrado),
            (turismo_municipio_cerrado, row_num_turismo_municipio_cerrado),
            (turismo_genero_cerrado, row_num_turismo_genero_cerrado),
            (turismo_motivo_cerrado, row_num_turismo_motivo_cerrado)
        ]

        # Listas para almacenar los DataFrames de 'Otros' y 'Totales'
        otros_df_list = []
        totales_df_list = []

        # Loop a través de los DataFrames para calcular 'Otros' y 'Total'
        for df, original_length in dataframes:
            # Obtener el nombre de la primera columna (e.g., 'PAIS_RESIDENCIA')
            primera_columna = df.columns[0]
            # Calcular valores para la categoría 'Otros'
            turismo_otros_cerrado_t_1 = turismo_total_cerrado_t_1 - df['SUMA_TURISMO_T_1'].sum()
            turismo_otros_cerrado_t = turismo_total_cerrado_t - df['SUMA_TURISMO_T'].sum()
            turismo_otros_cerrado_diferencia_porcentual = calcular_diferencia_porcentual(turismo_otros_cerrado_t, turismo_otros_cerrado_t_1)
            # Crear DataFrame para 'Otros'
            otros_df = pd.DataFrame({
                primera_columna: ['Otros'],
                'SUMA_TURISMO_T_1': [turismo_otros_cerrado_t_1],
                'SUMA_TURISMO_T': [turismo_otros_cerrado_t],
                'DIFERENCIA_PORCENTUAL_T': [turismo_otros_cerrado_diferencia_porcentual]
            })
            # Agregar el DataFrame 'Otros' a la lista
            otros_df_list.append(otros_df)
            # Crear DataFrame para 'Total'
            totales_df = pd.DataFrame({
                primera_columna: ['TOTAL'],
                'SUMA_TURISMO_T_1': [turismo_total_cerrado_t_1],
                'SUMA_TURISMO_T': [turismo_total_cerrado_t],
                'DIFERENCIA_PORCENTUAL_T': [turismo_total_cerrado_diferencia_porcentual]
            })
            # Agregar el DataFrame 'Total' a la lista
            totales_df_list.append(totales_df)

        # Calcular la participación porcentual y concatenar los DataFrames
        for i, (df, original_length) in enumerate(dataframes):
            if df is turismo_genero_cerrado:
                # Para género, no se agrega 'Otros'
                df_final = pd.concat([df, totales_df_list[i]], ignore_index=True)
            else:
                if original_length <= 5:
                    df_final = pd.concat([df, totales_df_list[i]], ignore_index=True)
                else:
                    df_final = pd.concat([df, otros_df_list[i], totales_df_list[i]], ignore_index=True)
            # Calcular participación porcentual
            df_final = calcular_participacion_porcentual(df_final, 'SUMA_TURISMO_T', turismo_total_cerrado_t, 'PARTICIPACION_T')
            # Obtener el nombre de la primera columna
            primera_columna = df_final.columns[0]
            # Almacenar el DataFrame final en el diccionario correspondiente
            turismo_cerrado[primera_columna] = df_final

        # Agregar los datos al diccionario de resumen
        # Para países
        if 'PAIS_RESIDENCIA' in turismo_cerrado:
            datos_resumen['TURISMO CERRADO PAISES'] = {}
            for _, row in turismo_cerrado['PAIS_RESIDENCIA'].iterrows():
                unidad = row['PAIS_RESIDENCIA']
                entry = {
                    'sum_turismo_t_1': row['SUMA_TURISMO_T_1'],
                    'sum_turismo_t': row['SUMA_TURISMO_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T'],
                    'participacion_t': row['PARTICIPACION_T']
                }
                datos_resumen['TURISMO CERRADO PAISES'].setdefault(unidad, []).append(entry)
        # Para departamentos
        if 'DPTO_HOSPEDAJE' in turismo_cerrado:
            datos_resumen['TURISMO CERRADO DEPARTAMENTOS'] = {}
            for _, row in turismo_cerrado['DPTO_HOSPEDAJE'].iterrows():
                unidad = row['DPTO_HOSPEDAJE']
                entry = {
                    'sum_turismo_t_1': row['SUMA_TURISMO_T_1'],
                    'sum_turismo_t': row['SUMA_TURISMO_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL_T'],
                    'participacion_t': row['PARTICIPACION_T']
                }
                datos_resumen['TURISMO CERRADO DEPARTAMENTOS'].setdefault(unidad, []).append(entry)

    # Ejecutar consultas y procesar datos para el año 'CORRIDO' si hay datos disponibles
    if dict_verificacion['turismo_corrido'] == 'CON DATOS DE TURISMO CORRIDO':
        # Ejecutar las consultas y almacenar los resultados en DataFrames de pandas
        turismo_paises_corrido = pd.DataFrame(session.sql(query_paises_turismo_paises_corrido).collect())
        turismo_departamentos_corrido = pd.DataFrame(session.sql(query_paises_turismo_departamentos_corrido).collect())
        turismo_municipio_corrido = pd.DataFrame(session.sql(query_paises_turismo_municipio_corrido).collect())
        turismo_genero_corrido = pd.DataFrame(session.sql(query_paises_turismo_genero_corrido).collect())
        turismo_motivo_corrido = pd.DataFrame(session.sql(query_paises_turismo_motivo_corrido).collect())

        # Calcular el número de filas en cada DataFrame
        row_num_turismo_paises_corrido = turismo_paises_corrido.shape[0]
        row_num_turismo_departamentos_corrido = turismo_departamentos_corrido.shape[0]
        row_num_turismo_municipio_corrido = turismo_municipio_corrido.shape[0]
        row_num_turismo_genero_corrido = turismo_genero_corrido.shape[0]
        row_num_turismo_motivo_corrido = turismo_motivo_corrido.shape[0]

        # Calcular los totales generales
        turismo_total_corrido_t_1 = turismo_paises_corrido['SUMA_TURISMO_T_1'].sum()
        turismo_total_corrido_t = turismo_paises_corrido['SUMA_TURISMO_T'].sum()
        turismo_total_corrido_diferencia_porcentual = calcular_diferencia_porcentual(turismo_total_corrido_t, turismo_total_corrido_t_1)

        # Tomar los top 5 registros en cada DataFrame
        turismo_paises_corrido = turismo_paises_corrido.head(5)
        turismo_departamentos_corrido = turismo_departamentos_corrido.head(5)
        turismo_municipio_corrido = turismo_municipio_corrido.head(5)
        turismo_motivo_corrido = turismo_motivo_corrido.head(5)

        # Crear listas de DataFrames y sus tamaños originales
        dataframes_corrido = [
            (turismo_paises_corrido, row_num_turismo_paises_corrido),
            (turismo_departamentos_corrido, row_num_turismo_departamentos_corrido),
            (turismo_municipio_corrido, row_num_turismo_municipio_corrido),
            (turismo_genero_corrido, row_num_turismo_genero_corrido),
            (turismo_motivo_corrido, row_num_turismo_motivo_corrido)
        ]

        # Listas para almacenar los DataFrames de 'Otros' y 'Totales'
        otros_corrido_df_list = []
        totales_corrido_df_list = []

        # Loop a través de los DataFrames para calcular 'Otros' y 'Total'
        for df, original_length in dataframes_corrido:
            # Obtener el nombre de la primera columna
            primera_columna = df.columns[0]
            # Calcular valores para la categoría 'Otros'
            turismo_otros_corrido_t_1 = turismo_total_corrido_t_1 - df['SUMA_TURISMO_T_1'].sum()
            turismo_otros_corrido_t = turismo_total_corrido_t - df['SUMA_TURISMO_T'].sum()
            turismo_otros_corrido_diferencia_porcentual = calcular_diferencia_porcentual(turismo_otros_corrido_t, turismo_otros_corrido_t_1)
            # Crear DataFrame para 'Otros'
            otros_df = pd.DataFrame({
                primera_columna: ['Otros'],
                'SUMA_TURISMO_T_1': [turismo_otros_corrido_t_1],
                'SUMA_TURISMO_T': [turismo_otros_corrido_t],
                'DIFERENCIA_PORCENTUAL': [turismo_otros_corrido_diferencia_porcentual]
            })
            # Agregar el DataFrame 'Otros' a la lista
            otros_corrido_df_list.append(otros_df)
            # Crear DataFrame para 'Total'
            totales_df = pd.DataFrame({
                primera_columna: ['TOTAL'],
                'SUMA_TURISMO_T_1': [turismo_total_corrido_t_1],
                'SUMA_TURISMO_T': [turismo_total_corrido_t],
                'DIFERENCIA_PORCENTUAL': [turismo_total_corrido_diferencia_porcentual]
            })
            # Agregar el DataFrame 'Total' a la lista
            totales_corrido_df_list.append(totales_df)

        # Calcular la participación porcentual y concatenar los DataFrames
        for i, (df, original_length) in enumerate(dataframes_corrido):
            if original_length <= 5:
                df_final = pd.concat([df, totales_corrido_df_list[i]], ignore_index=True)
            else:
                df_final = pd.concat([df, otros_corrido_df_list[i], totales_corrido_df_list[i]], ignore_index=True)
            # Calcular participación porcentual
            df_final = calcular_participacion_porcentual(df_final, 'SUMA_TURISMO_T', turismo_total_corrido_t, 'PARTICIPACION_T')
            # Obtener el nombre de la primera columna
            primera_columna = df_final.columns[0]
            # Almacenar el DataFrame final en el diccionario correspondiente
            turismo_corrido[primera_columna] = df_final

        # Agregar los datos al diccionario de resumen
        # Para países
        if 'PAIS_RESIDENCIA' in turismo_corrido:
            datos_resumen['TURISMO CORRIDO PAISES'] = {}
            for _, row in turismo_corrido['PAIS_RESIDENCIA'].iterrows():
                unidad = row['PAIS_RESIDENCIA']
                entry = {
                    'sum_turismo_t_1': row['SUMA_TURISMO_T_1'],
                    'sum_turismo_t': row['SUMA_TURISMO_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                    'participacion_t': row['PARTICIPACION_T']
                }
                datos_resumen['TURISMO CORRIDO PAISES'].setdefault(unidad, []).append(entry)
        # Para departamentos
        if 'DPTO_HOSPEDAJE' in turismo_corrido:
            datos_resumen['TURISMO CORRIDO DEPARTAMENTOS'] = {}
            for _, row in turismo_corrido['DPTO_HOSPEDAJE'].iterrows():
                unidad = row['DPTO_HOSPEDAJE']
                entry = {
                    'sum_turismo_t_1': row['SUMA_TURISMO_T_1'],
                    'sum_turismo_t': row['SUMA_TURISMO_T'],
                    'diferencia_porcentual': row['DIFERENCIA_PORCENTUAL'],
                    'participacion_t': row['PARTICIPACION_T']
                }
                datos_resumen['TURISMO CORRIDO DEPARTAMENTOS'].setdefault(unidad, []).append(entry)
        
    # Retornar todos los resultados en un diccionario
    return {
        'TURISMO CERRADO': turismo_cerrado,
        'TURISMO CORRIDO': turismo_corrido,
        'RESUMEN': datos_resumen
    }

def get_data_oportunidades_conectividad(session, geo_params, dict_verificacion):
    """
    Obtiene y procesa datos de oportunidades y conectividad desde Snowflake, basándose en los parámetros geográficos especificados.
    La función recupera datos de oportunidades de exportación, inversión y turismo, así como datos de conectividad aérea,
    dependiendo de la agrupación geográfica y la disponibilidad de datos.

    Parámetros:
    - session (snowflake.snowpark.Session): Sesión activa en Snowflake.
    - geo_params (dict): Parámetros geográficos obtenidos de la función get_data_parametros().
    - dict_verificacion (dict): Diccionario de verificación obtenido de la función verif_ejes().

    Retorna:
    - dict: Un diccionario que contiene los datos de 'CONECTIVIDAD' y 'OPORTUNIDADES' procesados.
    """

    ######################################
    # OBTENER PARÁMETROS SEGÚN SEA EL CASO
    ######################################

    # Extraer el parámetro 'AGRUPACION' y 'UNIDAD' de geo_params
    AGRUPACION = geo_params['AGRUPACION']
    UNIDAD = geo_params['UNIDAD'][0]  # Tomamos el primer elemento de la lista
    
    # Preparar los parámetros para los datos de turismo según la agrupación
    if AGRUPACION == 'DEPARTAMENTOS':
        # Obtener la lista de códigos de departamentos
        DEPARTAMENTOS_TURISMO = [departamento for departamento in geo_params['UNIDAD_COD'] if departamento is not None]
        # Formatear la lista para usarla en las consultas SQL
        DEPARTAMENTOS_TURISMO_sql = ', '.join(f"'{departamento}'" for departamento in DEPARTAMENTOS_TURISMO)
    
    ##############
    # CONECTIVIDAD
    ##############
    # Inicializar el diccionario para almacenar los datos de conectividad
    conectividad = {}

    # Los datos de conectividad solo se utilizan si la agrupación es por departamentos y hay datos disponibles
    if AGRUPACION == 'DEPARTAMENTOS' and dict_verificacion.get('conectividad') == "CON DATOS DE CONECTIVIDAD":
        # Construir la consulta SQL para obtener los datos de conectividad
        query_conectividad = """
            SELECT A.AEROLINEA AS "Aerolínea",
                   A.CIUDAD_ORIGEN AS "Ciudad Origen",
                   A.CIUDAD_DESTINO AS "Ciudad Destino",
                   A.FRECUENCIAS AS "Frecuencias",
                   A.SEMANA AS "Semana de análisis"
            FROM DOCUMENTOS_COLOMBIA.TURISMO.CONECTIVIDAD AS A
            WHERE 1 = 1
        """
        # Agregar el filtro por departamentos teniendo en cuenta que Bogotá se debe cambiar a Cundinamarca
        if UNIDAD == 'Bogotá':
            query_conectividad += f" AND A.COD_DIVIPOLA_DEPARTAMENTO_DESTINO IN ('25')"
        elif UNIDAD not in ['Bogotá']:
            query_conectividad += f" AND A.COD_DIVIPOLA_DEPARTAMENTO_DESTINO IN ({DEPARTAMENTOS_TURISMO_sql})"

        # Ejecutar la consulta y almacenar los resultados en un DataFrame de pandas
        df_conectividad = pd.DataFrame(session.sql(query_conectividad).collect())

        # Agregar el DataFrame al diccionario de conectividad
        conectividad['CONECTIVIDAD'] = df_conectividad

    ###############
    # OPORTUNIDADES
    ###############
    # Inicializar el diccionario para almacenar los datos de oportunidades
    oportunidades = {}

    # Obtener los países llaves de exportación en caso de que la agrupación sea de turismo:
    if AGRUPACION == 'TLCS':
        # Obtener países llave
        df_llave = pd.DataFrame(
                        session.sql(f"""
                            SELECT DISTINCT A.PAIS_LLAVE_EXPORTACIONES 
                            FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES AS A 
                            WHERE A.NOMBRE_TLC = '{UNIDAD}'
                        """).collect()
                    )    
        # Obtener string de búsqueda de países
        PAISES = df_llave['PAIS_LLAVE_EXPORTACIONES'].dropna().unique().tolist()
        PAISES_sql = ', '.join(f"'{pais}'" for pais in PAISES)

    # Verificar si la agrupación es válida para oportunidades y si hay datos disponibles
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'DEPARTAMENTOS', 'COLOMBIA']:
        # Oportunidades de Exportación
        if dict_verificacion.get('oportunidades_exportacion') == "CON OPORTUNIDADES":
            # Construir la consulta SQL para oportunidades de exportación
            query_oportunidades_exportacion = """
                SELECT DISTINCT A.CADENA,
                       LOWER(A.SUBSECTOR) AS SUBSECTOR
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
                WHERE A.EJE = 'Exportaciones'
            """
            # Agregar filtros según la agrupación
            # Continentes
            if AGRUPACION == 'CONTINENTES':
                query_oportunidades_exportacion += f" AND A.CONTINENTE IN ('{UNIDAD}')"
            # HUBS
            elif AGRUPACION == 'HUBS':
                query_oportunidades_exportacion += f" AND A.HUB IN ('{UNIDAD}')"
            # PAISES
            elif AGRUPACION == 'PAISES':
                query_oportunidades_exportacion += f" AND A.PAIS IN ('{UNIDAD}')"
            # TLCS
            elif AGRUPACION == 'TLCS':
                query_oportunidades_exportacion += f" AND A.PAIS IN ({PAISES_sql})"
            # Departamento                
            elif AGRUPACION == 'DEPARTAMENTOS' and DEPARTAMENTOS_TURISMO:
                query_oportunidades_exportacion += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
            # Ordenar
            query_oportunidades_exportacion += " ORDER BY 1, 2 ASC"

            # Ejecutar la consulta y almacenar los resultados en un DataFrame
            oportunidades_exportacion_df = pd.DataFrame(session.sql(query_oportunidades_exportacion).collect())
            # Agregar el DataFrame al diccionario de oportunidades
            oportunidades['EXPORTACIONES'] = oportunidades_exportacion_df

        # Oportunidades de Inversión
        if dict_verificacion.get('oportunidades_inversion') == "CON OPORTUNIDADES":
            # Construir la consulta SQL para oportunidades de inversión
            query_oportunidades_ied = """
                SELECT DISTINCT A.CADENA,
                       LOWER(A.SUBSECTOR) AS SUBSECTOR
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
                WHERE A.EJE = 'Inversión'
            """
            # Agregar filtros según la agrupación
            # Continentes
            if AGRUPACION == 'CONTINENTES':
                query_oportunidades_ied += f" AND A.CONTINENTE IN ('{UNIDAD}')"
            # HUBS
            elif AGRUPACION == 'HUBS':
                query_oportunidades_ied += f" AND A.HUB IN ('{UNIDAD}')"
            # PAISES
            elif AGRUPACION == 'PAISES':
                query_oportunidades_ied += f" AND A.PAIS IN ('{UNIDAD}')"
            # TLCS
            elif AGRUPACION == 'TLCS':
                query_oportunidades_ied += f" AND A.PAIS IN ({PAISES_sql})"
            # Departamento                
            elif AGRUPACION == 'DEPARTAMENTOS' and DEPARTAMENTOS_TURISMO:
                query_oportunidades_ied += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
            # Ordenar
            query_oportunidades_ied += " ORDER BY 1, 2 ASC"

            # Ejecutar la consulta y almacenar los resultados en un DataFrame
            oportunidades_inversion_df = pd.DataFrame(session.sql(query_oportunidades_ied).collect())
            # Agregar el DataFrame al diccionario de oportunidades
            oportunidades['INVERSION'] = oportunidades_inversion_df

        # Oportunidades de Turismo
        if dict_verificacion.get('oportunidades_turismo') == "CON OPORTUNIDADES":
            # Construir la consulta SQL para oportunidades de turismo
            query_oportunidades_turismo = """
                SELECT DISTINCT LOWER(A.SECTOR) AS SECTOR,
                       LOWER(A.SUBSECTOR) AS SUBSECTOR
                FROM DOCUMENTOS_COLOMBIA.EXPORTACIONES.OPORTUNIDADES AS A
                WHERE A.EJE = 'Turismo'
            """
            # Agregar filtros según la agrupación
            # Continentes
            if AGRUPACION == 'CONTINENTES':
                query_oportunidades_turismo += f" AND A.CONTINENTE IN ('{UNIDAD}')"
            # HUBS
            elif AGRUPACION == 'HUBS':
                query_oportunidades_turismo += f" AND A.HUB IN ('{UNIDAD}')"
            # PAISES
            elif AGRUPACION == 'PAISES':
                query_oportunidades_turismo += f" AND A.PAIS IN ('{UNIDAD}')"
            # TLCS
            elif AGRUPACION == 'TLCS':
                query_oportunidades_turismo += f" AND A.PAIS IN ({PAISES_sql})"
            # Departamento                
            elif AGRUPACION == 'DEPARTAMENTOS' and DEPARTAMENTOS_TURISMO:
                query_oportunidades_turismo += f" AND A.COD_DIVIPOLA_DEPARTAMENTO IN ({DEPARTAMENTOS_TURISMO_sql})"
            # Ordenar
            query_oportunidades_turismo += " ORDER BY 1, 2 ASC"

            # Ejecutar la consulta y almacenar los resultados en un DataFrame
            oportunidades_turismo_df = pd.DataFrame(session.sql(query_oportunidades_turismo).collect())
            # Agregar el DataFrame al diccionario de oportunidades
            oportunidades['TURISMO'] = oportunidades_turismo_df

    # Retornar todos los resultados en un diccionario
    return {
        'CONECTIVIDAD': conectividad,
        'OPORTUNIDADES': oportunidades
    }

def get_data(session, geo_params, dict_verificacion):
    """
    Esta función recopila y procesa datos relacionados con exportaciones, inversión, turismo, conectividad y oportunidades,
    utilizando parámetros predefinidos. Devuelve un diccionario con los datos recopilados y procesados.

    Parámetros:
    - session: Objeto de sesión de Snowflake.
    - geo_params: Parámetros geográficos generados externamente.
    - dict_verificacion: Diccionario que indica la disponibilidad de datos para los diferentes ejes.

    Retorna:
    - Un diccionario que contiene todos los datos recopilados y procesados.
    """

    ########################################
    # EJECUTAR FUNCIONES PARA OBTENER DATOS
    ########################################

    # Obtener datos de exportaciones
    data_exportaciones = get_data_exportaciones(session, geo_params, dict_verificacion)

    # Obtener datos de inversión
    data_inversion = get_data_inversion(session, geo_params, dict_verificacion)

    # Obtener datos de turismo
    data_turismo = get_data_turismo(session, geo_params, dict_verificacion)

    # Obtener datos de conectividad y oportunidades
    data_oportunidades_conectividad = get_data_oportunidades_conectividad(session, geo_params, dict_verificacion)

    #######################################
    # COMPILAR TODOS LOS RESULTADOS
    #######################################

    # Compilar los resultados en un solo diccionario
    resultados = {
        'TOTALES': data_exportaciones.get('TOTALES', {}),
        'TIPOS': data_exportaciones.get('TIPOS', {}),
        'CATEGORIAS CERRADO': data_exportaciones.get('CATEGORIAS CERRADO', {}),
        'CATEGORIAS CORRIDO': data_exportaciones.get('CATEGORIAS CORRIDO', {}),
        'EMPRESAS': data_exportaciones.get('EMPRESAS', {}),
        'CONTEO EMPRESAS': data_exportaciones.get('CONTEO EMPRESAS', {}),
        'TOTALES PESO': data_exportaciones.get('TOTALES PESO', {}),
        'TIPOS PESO': data_exportaciones.get('TIPOS PESO', {}),
        'MEDIOS PESO MINERO': data_exportaciones.get('MEDIOS PESO MINERO', {}),
        'MEDIOS PESO NO MINERO': data_exportaciones.get('MEDIOS PESO NO MINERO', {}),
        'IED ACTIVIDADES COLOMBIA': data_inversion.get('IED ACTIVIDADES COLOMBIA', {}),
        'IED PAISES': data_inversion.get('IED PAISES', {}),
        'IED TOTAL': data_inversion.get('IED TOTAL', {}),
        'ICE PAISES': data_inversion.get('ICE PAISES', {}),
        'ICE TOTAL': data_inversion.get('ICE TOTAL', {}),
        'UNCTAD' : data_inversion.get('UNCTAD', {}),
        'BALANZA': data_exportaciones.get('BALANZA', {}), 
        'TURISMO CERRADO': data_turismo.get('TURISMO CERRADO', {}),
        'TURISMO CORRIDO': data_turismo.get('TURISMO CORRIDO', {}),
        'CONECTIVIDAD': data_oportunidades_conectividad.get('CONECTIVIDAD', {}),
        'OPORTUNIDADES': data_oportunidades_conectividad.get('OPORTUNIDADES', {}),
        'RESUMEN': {
            **data_exportaciones.get('RESUMEN', {}),
            **data_inversion.get('RESUMEN', {}),
            **data_turismo.get('RESUMEN', {}),
        }
    }

    # Retornar el diccionario con todos los resultados
    return resultados


def get_parameters_exportaciones(session):
    """
    Obtiene los parámetros de año cerrado y año corrido para exportaciones desde la base de datos de Snowflake.
    
    Parámetros:
    - session: Objeto de sesión de Snowflake.
    
    Retorna:
    - dict: Un diccionario con los parámetros 'T' y 'T_1' para año cerrado y año corrido, así como información adicional.
    """
    # Consulta de parámetros para año cerrado
    query_cerrado = """
    SELECT
        MAX(CASE WHEN PARAMETRO = 'Año cerrado (T-1)' THEN VALOR ELSE NULL END) AS T_1_YEAR,
        MAX(CASE WHEN PARAMETRO = 'Año cerrado (T)' THEN VALOR ELSE NULL END) AS T_YEAR
    FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS
    WHERE EJE = 'Exportaciones'
      AND PARAMETRO IN ('Año cerrado (T-1)', 'Año cerrado (T)');
    """
    # Ejecutar la consulta y obtener los resultados
    params_cerrado_df = session.sql(query_cerrado).to_pandas()
    params_cerrado = params_cerrado_df.iloc[0]
    
    # Consulta de parámetros para año corrido
    query_corrido = """
    SELECT
        MAX(CASE WHEN PARAMETRO = 'Año corrido (T-1)' THEN VALOR ELSE NULL END) AS T_1_YEAR,
        MAX(CASE WHEN PARAMETRO = 'Año corrido (T)' THEN VALOR ELSE NULL END) AS T_YEAR,
        MAX(CASE WHEN PARAMETRO = 'Mes corrido texto (T)' THEN VALOR ELSE NULL END) AS MES_T
    FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS
    WHERE EJE = 'Exportaciones'
      AND PARAMETRO IN ('Año corrido (T-1)', 'Año corrido (T)', 'Mes corrido texto (T)');
    """
    # Ejecutar la consulta y obtener los resultados
    params_corrido_df = session.sql(query_corrido).to_pandas()
    params_corrido = params_corrido_df.iloc[0]
    
    # Función auxiliar para obtener el año del periodo corrido
    def get_year(year_str):
        """
        Extrae el año de una cadena que puede contener información adicional entre paréntesis.
        
        Parámetros:
        - year_str (str): Cadena que representa el año, posiblemente con paréntesis.
        
        Retorna:
        - str: El año extraído.
        """
        return year_str.split('(')[0].strip()
    
    # Construir el diccionario de resultados
    return {
        'cerrado': {
            'T_1': params_cerrado['T_1_YEAR'],
            'T': params_cerrado['T_YEAR']
        },
        'corrido': {
            'T_1': params_corrido['T_1_YEAR'],
            'T': params_corrido['T_YEAR'],
            'MES_T': params_corrido['MES_T'],
            'T_1_YEAR': get_year(params_corrido['T_1_YEAR']),
            'T_YEAR': get_year(params_corrido['T_YEAR'])
        }
    }



def get_parameters_inversion(session):
    """
    Obtiene los parámetros de año cerrado y año corrido para inversión desde la base de datos de Snowflake.
    
    Parámetros:
    - session: Objeto de sesión de Snowflake.
    
    Retorna:
    - dict: Un diccionario con los parámetros 'T' y 'T_1' para año cerrado y año corrido, incluyendo información de trimestres.
    """
    # Consulta de parámetros para año cerrado
    query_cerrado = """
    SELECT
        MAX(CASE WHEN PARAMETRO = 'Año cerrado (T-1)' THEN VALOR ELSE NULL END) AS T_1_YEAR,
        MAX(CASE WHEN PARAMETRO = 'Año cerrado (T)' THEN VALOR ELSE NULL END) AS T_YEAR
    FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS
    WHERE EJE = 'Inversión'
      AND PARAMETRO IN ('Año cerrado (T-1)', 'Año cerrado (T)');
    """
    # Ejecutar la consulta y obtener los resultados
    params_cerrado_df = session.sql(query_cerrado).to_pandas()
    params_cerrado = params_cerrado_df.iloc[0]
    
    # Consulta de parámetros para año corrido
    query_corrido = """
    SELECT
        MAX(CASE WHEN PARAMETRO = 'Año corrido (T-1)' THEN VALOR ELSE NULL END) AS T_1_YEAR,
        MAX(CASE WHEN PARAMETRO = 'Año corrido (T)' THEN VALOR ELSE NULL END) AS T_YEAR
    FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS
    WHERE EJE = 'Inversión'
      AND PARAMETRO IN ('Año corrido (T-1)', 'Año corrido (T)');
    """
    # Ejecutar la consulta y obtener los resultados
    params_corrido_df = session.sql(query_corrido).to_pandas()
    params_corrido = params_corrido_df.iloc[0]
    
    # Función auxiliar para obtener el nombre del trimestre
    def get_trimestre_name(year_quarter):
        """
        Convierte el número de trimestre en su nombre ordinal en español.
        
        Parámetros:
        - year_quarter (str): Cadena con el formato 'YYYY-Q', donde Q es el número del trimestre.
        
        Retorna:
        - str: Nombre del trimestre en español.
        """
        quarter = year_quarter.split('-')[-1]
        trimestres = {
            '1': 'primer',
            '2': 'segundo',
            '3': 'tercer',
            '4': 'cuarto'
        }
        return trimestres.get(quarter, '')
    
    # Función auxiliar para obtener el año
    def get_year(year_quarter):
        """
        Extrae el año de una cadena que contiene año y trimestre.
        
        Parámetros:
        - year_quarter (str): Cadena con el formato 'YYYY-Q'.
        
        Retorna:
        - str: El año extraído.
        """
        return year_quarter.split('-')[0]
    
    # Construir el diccionario de resultados
    return {
        'cerrado': {
            'T_1': params_cerrado['T_1_YEAR'],
            'T': params_cerrado['T_YEAR']
        },
        'corrido': {
            'T_1': params_corrido['T_1_YEAR'],
            'T': params_corrido['T_YEAR'],
            'T_1_TRIMESTER_NUMBER': params_corrido['T_1_YEAR'].split('-')[-1],
            'T_TRIMESTER_NUMBER': params_corrido['T_YEAR'].split('-')[-1],
            'T_1_TRIMESTER_NAME': get_trimestre_name(params_corrido['T_1_YEAR']),
            'T_TRIMESTER_NAME': get_trimestre_name(params_corrido['T_YEAR']),
            'T_1_YEAR': get_year(params_corrido['T_1_YEAR']),
            'T_YEAR': get_year(params_corrido['T_YEAR'])
        }
    }


def get_parameters_turismo(session):
    """
    Obtiene los parámetros de año cerrado y mes corrido para turismo desde la base de datos de Snowflake.
    
    Parámetros:
    - session: Objeto de sesión de Snowflake.
    
    Retorna:
    - dict: Un diccionario con los parámetros 'T', 'T_1' para año cerrado y corrido, y detalles del mes.
    """
    # Consulta de parámetros de turismo
    query_turismo = """
    SELECT
        MAX(CASE WHEN PARAMETRO = 'Año cerrado (T-1)' THEN VALOR ELSE NULL END) AS T_1_YEAR,
        MAX(CASE WHEN PARAMETRO = 'Año cerrado (T)' THEN VALOR ELSE NULL END) AS T_YEAR,
        MAX(CASE WHEN PARAMETRO = 'Año corrido (T-1)' THEN VALOR ELSE NULL END) AS T_1_YEAR_CORRIDO,
        MAX(CASE WHEN PARAMETRO = 'Año corrido (T)' THEN VALOR ELSE NULL END) AS T_YEAR_CORRIDO,
        MAX(CASE WHEN PARAMETRO = 'Mes corrido' THEN VALOR ELSE NULL END) AS T_MONTH_CORRIDO
    FROM DOCUMENTOS_COLOMBIA.PARAMETROS.PARAMETROS
    WHERE EJE = 'Turismo';
    """
    # Ejecutar la consulta y obtener los resultados
    params_turismo_df = session.sql(query_turismo).to_pandas()
    params_turismo = params_turismo_df.iloc[0]
    
    # Diccionarios para los meses en español
    meses_abreviados = {
        1: "Ene",
        2: "Feb",
        3: "Mar",
        4: "Abr",
        5: "May",
        6: "Jun",
        7: "Jul",
        8: "Ago",
        9: "Sep",
        10: "Oct",
        11: "Nov",
        12: "Dic"
    }
    meses_completos = {
        1: "Enero",
        2: "Febrero",
        3: "Marzo",
        4: "Abril",
        5: "Mayo",
        6: "Junio",
        7: "Julio",
        8: "Agosto",
        9: "Septiembre",
        10: "Octubre",
        11: "Noviembre",
        12: "Diciembre"
    }
    
    # Obtener el número de mes y convertirlo a entero
    month_number = int(params_turismo['T_MONTH_CORRIDO'])
    # Obtener el nombre abreviado y completo del mes
    month_abbr = meses_abreviados.get(month_number, '')
    month_name = meses_completos.get(month_number, '')
    
    # Construir el diccionario de resultados
    return {
        'cerrado': {
            'T_1': params_turismo['T_1_YEAR'],
            'T': params_turismo['T_YEAR']
        },
        'corrido': {
            'T_1': params_turismo['T_1_YEAR_CORRIDO'],
            'T': params_turismo['T_YEAR_CORRIDO'],
            'T_MONTH': params_turismo['T_MONTH_CORRIDO'],
            'T_MONTH_NAME': month_abbr,
            'T_MONTH_NAME_FULL': month_name
        }
    }

def transform_year_column_name(col_name):
    """
    Transforma el nombre de una columna de año para un formato más amigable.
    
    Parámetros:
    col_name (str): Nombre de la columna que contiene el año y el período.

    Retorna:
    str: El nombre de la columna transformado si contiene paréntesis, de lo contrario, el nombre original.
    """
    if '(' in col_name and ')' in col_name:
        year, period = col_name.split('(')
        period = period.replace(')', '')
        return f'{period.strip()} {year.strip()}'
    return col_name

def format_columns_exportaciones(df):
    """
    Aplica el formato adecuado a las columnas de valor, peso, variación y participación en el DataFrame de exportaciones.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de exportaciones a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas formateadas.
    """

    # Iterar sobre las columnas del DataFrame
    for col in df.columns:
        # Formatear columnas de valor en USD y peso en toneladas
        if 'USD' in col or 'TONELADAS' in col:
            # Convertir a numérico, ignorando errores y redondear a 0 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(0)
            # Aplicar formato de separadores de miles y reemplazar puntos por comas
            df[col] = df[col].apply(
                lambda x: f"{x:,.0f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                if not pd.isnull(x) else x
            )

    # Formatear columna de variación porcentual si existe
    if 'Variación (%)' in df.columns:
        # Convertir a numérico y redondear a 1 decimal
        df['Variación (%)'] = pd.to_numeric(df['Variación (%)'], errors='coerce').round(1)
        # Añadir símbolo de porcentaje y reemplazar punto por coma
        df['Variación (%)'] = df['Variación (%)'].apply(
            lambda x: f"{x:.1f}%".replace('.', ',') if not pd.isnull(x) else x
        )

    # Formatear columnas de participación porcentual
    for col in df.columns:
        # Identificar columnas que comienzan con 'Participación (%)'
        if col.startswith('Participación (%)'):
            # Convertir a numérico y redondear a 1 decimal
            df[col] = pd.to_numeric(df[col], errors='coerce').round(1)
            # Añadir símbolo de porcentaje y reemplazar punto por coma
            df[col] = df[col].apply(
                lambda x: f"{x:.1f}%".replace('.', ',') if not pd.isnull(x) else x
            )

    # Devolver el DataFrame formateado
    return df


def format_columns_exportaciones_excel(df):
    """
    Prepara el DataFrame de exportaciones para exportación a Excel, formateando columnas de valor, peso, variación y participación.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de exportaciones a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas formateadas para Excel.
    """
    # Iterar sobre las columnas del DataFrame
    for col in df.columns:
        # Formatear columnas de valor en USD y peso en toneladas
        if 'USD' in col or 'TONELADAS' in col:
            # Convertir a numérico, ignorando errores y redondear a 2 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Formatear columna de variación porcentual si existe
    if 'Variación (%)' in df.columns:
        # Convertir a numérico y redondear a 2 decimales
        df['Variación (%)'] = pd.to_numeric(df['Variación (%)'], errors='coerce').round(2)

    # Formatear columnas de participación porcentual
    for col in df.columns:
        # Identificar columnas que comienzan con 'Participación (%)'
        if col.startswith('Participación (%)'):
            # Convertir a numérico y redondear a 2 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Devolver el DataFrame formateado para Excel
    return df


def format_columns_inversion(df):
    """
    Aplica el formato adecuado a las columnas de valor, variación y participación en el DataFrame de inversión.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de inversión a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas formateadas.
    """

    # Obtener el nombre de la primera columna
    first_col = df.columns[0]

    # Capitalizar los nombres en la primera columna si es 'País'
    #if 'País' in first_col:
    #    df[first_col] = df[first_col].apply(
    #        lambda x: ' '.join(word.capitalize() for word in x.split()) if isinstance(x, str) else x
    #    )

    # Iterar sobre las columnas del DataFrame
    for col in df.columns:
        # Formatear columnas de valor en USD
        if 'USD' in col:
            # Convertir a numérico y redondear a 1 decimal
            df[col] = pd.to_numeric(df[col], errors='coerce').round(1)
            # Aplicar formato de separadores de miles y reemplazar puntos por comas
            df[col] = df[col].apply(
                lambda x: f"{x:,.1f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                if not pd.isnull(x) else x
            )
        # Formatear columnas de participación y variación porcentual
        elif col.startswith('Participación (%)') or col.startswith('Variación (%)'):
            # Convertir a numérico y redondear a 1 decimal
            df[col] = pd.to_numeric(df[col], errors='coerce').round(1)
            # Añadir símbolo de porcentaje y reemplazar punto por coma
            df[col] = df[col].apply(
                lambda x: f"{x:.1f}%".replace('.', ',') if not pd.isnull(x) else x
            )

    # Devolver el DataFrame formateado
    return df


def format_columns_inversion_excel(df):
    """
    Prepara el DataFrame de inversión para exportación a Excel, formateando columnas de valor, variación y participación.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de inversión a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas formateadas para Excel.
    """
    # Obtener el nombre de la primera columna
    first_col = df.columns[0]

    # Capitalizar los nombres en la primera columna si es 'País' (Se elimina porque no es necesario, los nombres ya viene correctos.)
    #if 'País' in first_col:
    #    df[first_col] = df[first_col].apply(
    #        lambda x: ' '.join(word.capitalize() for word in x.split()) if isinstance(x, str) else x
    #    )

    # Iterar sobre las columnas del DataFrame
    for col in df.columns:
        # Formatear columnas de valor en USD
        if 'USD' in col:
            # Convertir a numérico y redondear a 2 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
        # Formatear columnas de participación y variación porcentual
        elif col.startswith('Participación (%)') or col.startswith('Variación (%)'):
            # Convertir a numérico y redondear a 2 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Devolver el DataFrame formateado para Excel
    return df


def format_columns_unctad(df):
    """
    Aplica el formato adecuado a las columnas numéricas en el DataFrame de la UNCTAD.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de la UNCTAD a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas numéricas formateadas.
    """

    # Iterar sobre las columnas del DataFrame
    for col in df.columns:
        # Verificar si la columna es numérica
        if pd.api.types.is_numeric_dtype(df[col]):
            # Convertir a numérico y redondear a 1 decimal
            df[col] = pd.to_numeric(df[col], errors='coerce').round(1)
            # Aplicar formato de separadores de miles y reemplazar puntos por comas
            df[col] = df[col].apply(
                lambda x: f"{x:,.1f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                if not pd.isnull(x) else x
            )

    # Devolver el DataFrame formateado
    return df


def format_columns_unctad_excel(df):
    """
    Prepara el DataFrame de la UNCTAD para exportación a Excel, formateando las columnas numéricas.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de la UNCTAD a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas numéricas formateadas para Excel.
    """

    # Iterar sobre las columnas del DataFrame
    for col in df.columns:
        # Verificar si la columna es numérica
        if pd.api.types.is_numeric_dtype(df[col]):
            # Convertir a numérico y redondear a 2 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Devolver el DataFrame formateado para Excel
    return df


def format_columns_balanza(df):
    """
    Aplica el formato adecuado a las columnas numéricas en el DataFrame de la Balanza Comercial.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de la Balanza Comercial a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas numéricas formateadas.
    """

    # Iterar sobre las columnas del DataFrame
    for col in df.columns:
        # Verificar si la columna es numérica
        if pd.api.types.is_numeric_dtype(df[col]):
            # Convertir a numérico y redondear a 0 decimal
            df[col] = pd.to_numeric(df[col], errors='coerce').round(0)
            # Aplicar formato de separadores de miles y reemplazar puntos por comas
            df[col] = df[col].apply(
                lambda x: f"{x:,.1f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                if not pd.isnull(x) else x
            )

    # Devolver el DataFrame formateado
    return df


def format_columns_balanza_excel(df):
    """
    Prepara el DataFrame de la Balanza Comercial para exportación a Excel, formateando las columnas numéricas.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de la Balanza Comercial a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas numéricas formateadas para Excel.
    """

    # Iterar sobre las columnas del DataFrame
    for col in df.columns:
        # Verificar si la columna es numérica
        if pd.api.types.is_numeric_dtype(df[col]):
            # Convertir a numérico y redondear a 0 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(0)

    # Devolver el DataFrame formateado para Excel
    return df


def format_columns_turismo(df):
    """
    Aplica el formato adecuado a las columnas del DataFrame de turismo, incluyendo valores, variaciones y participaciones.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de turismo a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas formateadas.
    """

    # Lista de posibles nombres para la primera columna que requieren capitalización
    columns_to_capitalize = ['País de residencia', 'Departamento de hospedaje', 'Ciudad de hospedaje', 'Motivo de viaje', 'Género']
    first_col = df.columns[0]

    # Capitalizar los nombres en la primera columna si corresponde (Se elimina porque no es necesario, los nombres ya viene correctos.)
    #if first_col in columns_to_capitalize:
    #    df[first_col] = df[first_col].apply(
    #        lambda x: ' '.join(word.capitalize() for word in x.split()) if isinstance(x, str) else x
    #    )

    # Formatear columnas de valores numéricos (años, meses, diferencias)
    for col in df.columns:
        if col.startswith('20') or col.startswith('Ene') or col.startswith('Diferencia'):
            # Convertir a numérico y redondear a 0 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(0)
            # Aplicar formato de separadores de miles y reemplazar puntos por comas
            df[col] = df[col].apply(
                lambda x: f"{x:,.0f}".replace(',', 'X').replace('.', ',').replace('X', '.')
                if not pd.isnull(x) else x
            )

    # Formatear columnas de participación y variación porcentual
    for col in df.columns:
        if col.startswith('Participación (%)') or col.startswith('Variación (%)') or col.startswith('Diferencia'):
            # Convertir a numérico y redondear a 1 decimal
            df[col] = pd.to_numeric(df[col], errors='coerce').round(1)
            # Añadir símbolo de porcentaje y reemplazar punto por coma
            df[col] = df[col].apply(
                lambda x: f"{x:.1f}%".replace('.', ',') if not pd.isnull(x) else x
            )

    # Devolver el DataFrame formateado
    return df


def format_columns_turismo_excel(df):
    """
    Prepara el DataFrame de turismo para exportación a Excel, formateando columnas de valor, variación y participación.

    Parámetros:
    - df (pandas.DataFrame): DataFrame de turismo a formatear.

    Retorna:
    - pandas.DataFrame: DataFrame con las columnas formateadas para Excel.
    """
    # Lista de posibles nombres para la primera columna que requieren capitalización
    columns_to_capitalize = ['País de residencia', 'Departamento de hospedaje', 'Ciudad de hospedaje', 'Motivo de viaje', 'Género']
    first_col = df.columns[0]

    # Capitalizar los nombres en la primera columna si corresponde (Se elimina porque no es necesario, los nombres ya viene correctos.)
    #if first_col in columns_to_capitalize:
    #    df[first_col] = df[first_col].apply(
    #        lambda x: ' '.join(word.capitalize() for word in x.split()) if isinstance(x, str) else x
    #    )

    # Formatear columnas de valores numéricos (años, meses)
    for col in df.columns:
        if col.startswith('20') or col.startswith('Ene'):
            # Convertir a numérico y redondear a 2 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Formatear columnas de participación y variación porcentual
    for col in df.columns:
        if col.startswith('Participación (%)') or col.startswith('Variación (%)') or col.startswith('Diferencia'):
            # Convertir a numérico y redondear a 2 decimales
            df[col] = pd.to_numeric(df[col], errors='coerce').round(2)

    # Devolver el DataFrame formateado para Excel
    return df

def obtener_paises_correlativa(session, eje):
    """
    Obtiene una lista de países según el eje especificado (Exportaciones, Inversión o Turismo) o datos UNCTAD por país o región.
    Consulta la base de datos de Snowflake para obtener los códigos y nombres de los países relacionados con cada eje.

    Parámetros:
    - session: Objeto de sesión de Snowflake para ejecutar las consultas.
    - eje (str): El eje de análisis ('EXPORTACIONES', 'INVERSION', 'TURISMO').

    Retorna:
    - pandas.DataFrame: Un DataFrame con los códigos y nombres de los países obtenidos de la consulta.
    """

    # Definir la consulta SQL según el eje especificado
    if eje == 'EXPORTACIONES':
        query = """
            SELECT DISTINCT A.PAIS_LLAVE_EXPORTACIONES, A.COUNTRY_OR_AREA AS COUNTRY_OR_AREA_UNSD 
            FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES AS A;
        """
    elif eje == 'INVERSION':
        query = """
            SELECT DISTINCT A.PAIS_INVERSION_BANREP, A.COUNTRY_OR_AREA AS COUNTRY_OR_AREA_UNSD 
            FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES AS A;
        """
    elif eje == 'TURISMO':
        query = """
            SELECT DISTINCT A.CODIGO_PAIS_MIGRACION, A.COUNTRY_OR_AREA AS COUNTRY_OR_AREA_UNSD 
            FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES AS A;
        """
    elif eje == 'UNCTAD_PAIS':
        query = """
            SELECT DISTINCT A.M49_CODE, A.COUNTRY_OR_AREA AS COUNTRY_OR_AREA_UNSD 
            FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES AS A;
        """
    elif eje == 'UNCTAD_CONTINENTE':
        query = """
            SELECT DISTINCT A.M49_CODE, A.COUNTRY_OR_AREA AS COUNTRY_OR_AREA_UNSD 
            FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.ST_PAISES AS A;
        """
    else:
        raise ValueError("El eje proporcionado no es válido. Debe ser 'EXPORTACIONES', 'INVERSION' o 'TURISMO'.")

    # Ejecutar la consulta y convertir los resultados en un DataFrame
    data = pd.DataFrame(session.sql(query).collect())

    return data

def obtener_departamentos_correlativa(session):
    """
    Obtiene una lista de departamentos y sus códigos DIAN desde la base de datos de Snowflake.
    Consulta la tabla 'DIAN_DEPARTAMENTOS' para extraer los nombres y códigos de los departamentos registrados.

    Parámetros:
    - session (snowflake.connector.SnowflakeConnection): Objeto de sesión de Snowflake para ejecutar la consulta.

    Retorna:
    - pandas.DataFrame: Un DataFrame con dos columnas: 'DEPARTAMENTO_DIAN' (nombre del departamento) y 
      'COD_DIAN_DEPARTAMENTO' (código del departamento según la DIAN).
    """
    # Definir la consulta SQL para obtener los departamentos y sus códigos DIAN
    query = """
    SELECT A.DEPARTAMENTO_DIAN,
           A.COD_DIAN_DEPARTAMENTO
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIAN_DEPARTAMENTOS AS A;
    """
    
    # Ejecutar la consulta en Snowflake y convertir el resultado en un DataFrame de pandas
    data = pd.DataFrame(session.sql(query).collect())
        
    return data


def obtener_municipios_correlativa(session):
    """
    Obtiene una lista de municipios y sus códigos DANE desde la base de datos de Snowflake.
    Consulta la tabla 'DIVIPOLA_MUNICIPIOS' para extraer los nombres y códigos de los municipios registrados.

    Parámetros:
    - session (snowflake.connector.SnowflakeConnection): Objeto de sesión de Snowflake para ejecutar la consulta.

    Retorna:
    - pandas.DataFrame: Un DataFrame con dos columnas: 'COD_DANE_MUNICIPIO' (código del municipio según DANE) y 
      'MUNICIPIO_DANE' (nombre del municipio según DANE).
    """
    # Definir la consulta SQL para obtener los municipios y sus códigos DANE
    query = """
    SELECT A.COD_DANE_MUNICIPIO,
           A.MUNICIPIO_DANE
    FROM DOCUMENTOS_COLOMBIA.GEOGRAFIA.DIVIPOLA_MUNICIPIOS AS A;
    """
    
    # Ejecutar la consulta en Snowflake y convertir el resultado en un DataFrame de pandas
    data = pd.DataFrame(session.sql(query).collect())
        
    return data

def format_number(value):
    """
    Formatea un número para que use coma como separador decimal y punto como separador de miles.

    Parámetros:
    - value (float): El número a formatear.

    Retorna:
    - str: El número formateado como cadena.
    """
    # Verificar si el valor es numérico
    if isinstance(value, numbers.Number):
        # Formatear el número con un decimal, usando coma como separador decimal y punto como separador de miles
        formatted_value = f"{value:,.2f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        return formatted_value
    else:
        # Si el valor no es numérico, se devuelve como está
        return value



def format_number_no_decimal(value):
    """
    Formatea un número para que use coma como separador decimal y punto como separador de miles, sin decimales.

    Parámetros:
    - value (float): El número a formatear.

    Retorna:
    - str: El número formateado como cadena sin decimales.
    """
    # Verificar si el valor es numérico
    if isinstance(value, (int, float, Decimal)):
        # Formatear el número sin decimales, usando coma como separador decimal y punto como separador de miles
        # Se utiliza una cadena de formato y reemplazos para ajustar los separadores
        formatted_value = f"{value:,.0f}".replace(',', 'X').replace('.', ',').replace('X', '.')
        return formatted_value
    else:
        # Si el valor no es numérico, se devuelve como está
        return value


def inversion_palabra(valor):
    """
    Determina si un valor es positivo o negativo y devuelve la palabra correspondiente.

    Parámetros:
    - valor (float): El valor numérico a evaluar.

    Retorna:
    - str: 'positivos' si el valor es mayor que 0, 'negativos' en caso contrario.
    """
    # Evaluar si el valor es mayor que cero
    if valor > 0:
        return "positivos"
    else:
        return "negativos"


def variacion_palabra(valor):
    """
    Determina si un valor representa una variación al alza o a la baja y devuelve la palabra correspondiente.

    Parámetros:
    - valor (float): El valor numérico de la variación.

    Retorna:
    - str: 'más' si el valor es mayor que 0, 'menos' en caso contrario.
    """
    # Evaluar si el valor es mayor que cero
    if valor > 0:
        return "más"
    else:
        return "menos"
    
def resumen_datos(data_dict, agrupacion, unidad, export_params, inversion_params, turismo_params, dict_verif):
    """
    Genera un resumen de datos de exportaciones, inversión y turismo.

    Parámetros:
    - data_dict: diccionario con los datos de exportaciones, inversión y turismo.
    - agrupacion: el nivel de agrupación para filtrar los datos.
    - unidad: nombre de la unidad de la agrupación para el resumen
    - export_params: parámetros para exportaciones.
    - inversion_params: parámetros para inversión.
    - turismo_params: parámetros para turismo.
    - dict_verif: diccionario con las marcas de verificación por los tres ejes

    Retorna:
    Un diccionario con las tablas de resumen y losz textos de resumen.
    """

    # Exportaciones:
    expo_columna_t_1_cerrado = f"{export_params['cerrado']['T_1']} (USD FOB millones)"
    expo_columna_t_cerrado = f"{export_params['cerrado']['T']} (USD FOB millones)"
    expo_variacion_cerrado = f"Variación (%) {transform_year_column_name(export_params['corrido']['T_1'])}"
    expo_columna_t_1_corrido = f"{transform_year_column_name(export_params['corrido']['T_1'])} (USD FOB millones)"
    expo_columna_t_corrido = f"{transform_year_column_name(export_params['corrido']['T'])} (USD FOB millones)"
    expo_variacion_corrido = f"Variación (%) {transform_year_column_name(export_params['corrido']['T'])}"


    # Crear la tabla con las variables y los datos del diccionario
    variables_expo = ['No Mineras', 'Mineras', 'Total']

    # Inicializar data_expo_cerrado y data_expo_corrido
    data_expo_cerrado = {}
    data_expo_corrido = {}

    # Verificar y construir la tabla de exportaciones cerradas
    if dict_verif['exportaciones_totales_cerrado'] == 'CON DATOS DE EXPORTACIONES TOTALES CERRADO':
        data_expo_cerrado = {
            'Tipo de exportación': variables_expo,
            expo_columna_t_1_cerrado: [data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO'].get(var, [{'sum_usd_t_1': 0}])[0]['sum_usd_t_1'] / 1e6 for var in variables_expo],
            expo_columna_t_cerrado: [data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO'].get(var, [{'sum_usd_t': 0}])[0]['sum_usd_t'] / 1e6 for var in variables_expo],
            expo_variacion_cerrado: [data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_expo]            
        }
    else:
        data_expo_cerrado = {
            'Tipo de exportación': variables_expo,
            expo_columna_t_1_cerrado: [0 for var in variables_expo],
            expo_columna_t_cerrado: [0 for var in variables_expo],
            expo_variacion_cerrado: [0 for var in variables_expo]
        }

    # Verificar y construir la tabla de exportaciones corridas
    if dict_verif['exportaciones_totales_corrido'] == 'CON DATOS DE EXPORTACIONES TOTALES CORRIDO':
        data_expo_corrido = {
            expo_columna_t_1_corrido: [data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO'].get(var, [{'sum_usd_t_1': 0}])[0]['sum_usd_t_1'] / 1e6 for var in variables_expo],
            expo_columna_t_corrido: [data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO'].get(var, [{'sum_usd_t': 0}])[0]['sum_usd_t'] / 1e6 for var in variables_expo],
            expo_variacion_corrido: [data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_expo]
        }
    else:
        data_expo_corrido = {
            'Tipo de exportación': variables_expo,
            expo_columna_t_1_corrido: [0 for var in variables_expo],
            expo_columna_t_corrido: [0 for var in variables_expo],
            expo_variacion_corrido: [0 for var in variables_expo]
        }

    # Unir los datos cerrados y corridos
    data_expo = {**data_expo_cerrado, **data_expo_corrido}

    # Convertir el diccionario a DataFrame 
    tab_resumen_expo = pd.DataFrame(data_expo)
    tab_resumen_expo = format_columns_inversion(tab_resumen_expo)

    # Columnas para inversión
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        inv_columna_t_1_cerrado = f"{inversion_params['cerrado']['T_1']} (USD millones)"
        inv_columna_t_cerrado = f"{inversion_params['cerrado']['T']} (USD millones)"
        inv_variacion_cerrado = f"Variación (%) {transform_year_column_name(inversion_params['cerrado']['T'])}"
        inv_columna_t_1_corrido = f"{transform_year_column_name(inversion_params['corrido']['T_1'])} (USD millones)"
        inv_columna_t_corrido = f"{transform_year_column_name(inversion_params['corrido']['T'])} (USD millones)"
        inv_variacion_corrido = f"Variación (%) {transform_year_column_name(inversion_params['corrido']['T'])}"

        # Inicializar data_ied_cerrado, data_ied_corrido, data_ice_cerrado y data_ice_corrido
        data_ied_cerrado = {}
        data_ied_corrido = {}
        data_ice_cerrado = {}
        data_ice_corrido = {}

        # Inicializar variables_ied
        variables_ied = ['TOTAL'] if agrupacion != 'PAISES' else []

        # Crear la tabla con las variables y los datos del diccionario para IED
        # Verificar y construir la tabla de IED cerradas
        if dict_verif['ied_cerrado'] == 'CON DATOS DE IED CERRADO':
            if agrupacion == 'PAISES':
                variables_ied = list(data_dict['RESUMEN']['IED CERRADO PAISES'].keys())
            data_ied_cerrado = {
                'Tipo de inversión': 'IED',
                inv_columna_t_1_cerrado: [data_dict['RESUMEN']['IED CERRADO PAISES'].get(var, [{'sum_inversion_t_1': 0}])[0]['sum_inversion_t_1'] for var in variables_ied],
                inv_columna_t_cerrado: [data_dict['RESUMEN']['IED CERRADO PAISES'].get(var, [{'sum_inversion_t': 0}])[0]['sum_inversion_t'] for var in variables_ied],
                inv_variacion_cerrado: [data_dict['RESUMEN']['IED CERRADO PAISES'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_ied]
            }
        else:
            data_ied_cerrado = {
                'Tipo de inversión': 'IED',
                inv_columna_t_1_cerrado: [0],
                inv_columna_t_cerrado: [0],
                inv_variacion_cerrado: [0]
            }

        # Verificar y construir la tabla de IED corridas
        if dict_verif['ied_corrido'] == 'CON DATOS DE IED CORRIDO':
            if agrupacion == 'PAISES' and not variables_ied:  # Para asegurarnos de que variables_ied esté llenado si no se llenó en el bloque anterior
                variables_ied = list(data_dict['RESUMEN']['IED CORRIDO PAISES'].keys())
            data_ied_corrido = {
                'Tipo de inversión': 'IED',
                inv_columna_t_1_corrido: [data_dict['RESUMEN']['IED CORRIDO PAISES'].get(var, [{'sum_inversion_t_1': 0}])[0]['sum_inversion_t_1'] for var in variables_ied],
                inv_columna_t_corrido: [data_dict['RESUMEN']['IED CORRIDO PAISES'].get(var, [{'sum_inversion_t': 0}])[0]['sum_inversion_t'] for var in variables_ied],
                inv_variacion_corrido: [data_dict['RESUMEN']['IED CORRIDO PAISES'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_ied]
            }
        else:
            data_ied_corrido = {
                'Tipo de inversión': 'IED',
                inv_columna_t_1_corrido: [0],
                inv_columna_t_corrido: [0],
                inv_variacion_corrido: [0]
            }

        # Crear la tabla con las variables y los datos del diccionario para ICE
        # Inicializar variables_ice
        variables_ice = ['TOTAL'] if agrupacion != 'PAISES' else []

        # Verificar y construir la tabla de ICE cerradas
        if dict_verif['ice_cerrado'] == 'CON DATOS DE ICE CERRADO':
            if agrupacion == 'PAISES':
                variables_ice = list(data_dict['RESUMEN']['ICE CERRADO PAISES'].keys())
            data_ice_cerrado = {
                'Tipo de inversión': 'ICE',
                inv_columna_t_1_cerrado: [data_dict['RESUMEN']['ICE CERRADO PAISES'].get(var, [{'sum_inversion_t_1': 0}])[0]['sum_inversion_t_1'] for var in variables_ice],
                inv_columna_t_cerrado: [data_dict['RESUMEN']['ICE CERRADO PAISES'].get(var, [{'sum_inversion_t': 0}])[0]['sum_inversion_t'] for var in variables_ice],
                inv_variacion_cerrado: [data_dict['RESUMEN']['ICE CERRADO PAISES'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_ice]
            }
        else:
            data_ice_cerrado = {
                'Tipo de inversión': 'ICE',
                inv_columna_t_1_cerrado: [0],
                inv_columna_t_cerrado: [0],
                inv_variacion_cerrado: [0]
            }

        # Verificar y construir la tabla de ICE corridas
        if dict_verif['ice_corrido'] == 'CON DATOS DE ICE CORRIDO':
            if agrupacion == 'PAISES' and not variables_ice:  # Para asegurarnos de que variables_ice esté llenado si no se llenó en el bloque anterior
                variables_ice = list(data_dict['RESUMEN']['ICE CORRIDO PAISES'].keys())
            data_ice_corrido = {
                'Tipo de inversión': 'ICE',
                inv_columna_t_1_corrido: [data_dict['RESUMEN']['ICE CORRIDO PAISES'].get(var, [{'sum_inversion_t_1': 0}])[0]['sum_inversion_t_1'] for var in variables_ice],
                inv_columna_t_corrido: [data_dict['RESUMEN']['ICE CORRIDO PAISES'].get(var, [{'sum_inversion_t': 0}])[0]['sum_inversion_t'] for var in variables_ice],
                inv_variacion_corrido: [data_dict['RESUMEN']['ICE CORRIDO PAISES'].get(var, [{'diferencia_porcentual': 0}])[0]['diferencia_porcentual'] for var in variables_ice]
            }
        else:
            data_ice_corrido = {
                'Tipo de inversión': 'ICE',
                inv_columna_t_1_corrido: [0],
                inv_columna_t_corrido: [0],
                inv_variacion_corrido: [0]
            }

        # Unir los datos de IED cerrados y corridos
        data_ied = {**data_ied_cerrado, **data_ied_corrido}

        # Convertir el diccionario a DataFrame 
        tab_resumen_ied = pd.DataFrame(data_ied)

        # Unir los datos de ICE cerrados y corridos
        data_ice = {**data_ice_cerrado, **data_ice_corrido}

        # Convertir el diccionario a DataFrame 
        tab_resumen_ice = pd.DataFrame(data_ice)

        # Concatenar los DataFrames de inversión
        inv_df = pd.concat([tab_resumen_ied, tab_resumen_ice])
        tab_resumen_inv = format_columns_inversion(inv_df)

    # Turismo
    tur_columna_t_1_cerrado = turismo_params['cerrado']['T_1']
    tur_columna_t_cerrado = turismo_params['cerrado']['T']
    tur_variacion_cerrado = f"Variación (%) {transform_year_column_name(turismo_params['corrido']['T_1'])}"
    tur_columna_t_1_corrido = f"Ene - {turismo_params['corrido']['T_MONTH_NAME']} {turismo_params['corrido']['T_1']}"
    tur_columna_t_corrido = f"Ene - {turismo_params['corrido']['T_MONTH_NAME']} {turismo_params['corrido']['T']}"
    tur_variacion_corrido = f"Variación (%) Ene - {turismo_params['corrido']['T_MONTH_NAME']} {turismo_params['corrido']['T']}"

    # Crear la tabla con las variables y los datos del diccionario para turismo
    variables_turismo = ['TOTAL']

    # Inicializar data_turismo_cerrado y data_turismo_corrido
    data_turismo_cerrado = {}
    data_turismo_corrido = {}

    # Verificar y construir la tabla de turismo cerradas
    if dict_verif['turismo_cerrado'] == 'CON DATOS DE TURISMO CERRADO':
        data_turismo_cerrado = {
            'Variable': 'Viajeros',
            tur_columna_t_1_cerrado: [data_dict['RESUMEN']['TURISMO CERRADO PAISES'][var][0]['sum_turismo_t_1'] for var in variables_turismo],
            tur_columna_t_cerrado: [data_dict['RESUMEN']['TURISMO CERRADO PAISES'][var][0]['sum_turismo_t'] for var in variables_turismo],
            tur_variacion_cerrado: [data_dict['RESUMEN']['TURISMO CERRADO PAISES'][var][0]['diferencia_porcentual'] for var in variables_turismo]
        }
    else:
        data_turismo_cerrado = {
            'Variable': 'Viajeros',
            tur_columna_t_1_cerrado: [0],
            tur_columna_t_cerrado: [0],
            tur_variacion_cerrado: [0]
        }

    # Verificar y construir la tabla de turismo corridas
    if dict_verif['turismo_corrido'] == 'CON DATOS DE TURISMO CORRIDO':
        data_turismo_corrido = {
            tur_columna_t_1_corrido: [data_dict['RESUMEN']['TURISMO CORRIDO PAISES'][var][0]['sum_turismo_t_1'] for var in variables_turismo],
            tur_columna_t_corrido: [data_dict['RESUMEN']['TURISMO CORRIDO PAISES'][var][0]['sum_turismo_t'] for var in variables_turismo],
            tur_variacion_corrido: [data_dict['RESUMEN']['TURISMO CORRIDO PAISES'][var][0]['diferencia_porcentual'] for var in variables_turismo]
        }
    else:
        data_turismo_corrido = {
            tur_columna_t_1_corrido: [0],
            tur_columna_t_corrido: [0],
            tur_variacion_corrido: [0]
        }

    # Unir los datos cerrados y corridos
    data_turismo = {**data_turismo_cerrado, **data_turismo_corrido}

    # Convertir el diccionario a DataFrame 
    tab_resumen_tur = pd.DataFrame(data_turismo)
    tab_resumen_tur = format_columns_turismo(tab_resumen_tur)
    
    # Crear el texto de exportaciones
    
    # Obtener datos cuando existen o llenarlos con cero
    # Cerrado total
    if dict_verif['exportaciones_totales_cerrado'] == 'CON DATOS DE EXPORTACIONES TOTALES CERRADO':
        exportaciones_totales_cerrado = data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO']['Total'][0]
        exportaciones_total_cerrado = exportaciones_totales_cerrado['sum_usd_t']
        exportaciones_variacion_total_cerrado = exportaciones_totales_cerrado['diferencia_porcentual']
        exportaciones_totales_cerrado_peso = data_dict['RESUMEN']['ST_CATEGORIAS_PESO_CERRADO']['Total'][0]
        exportaciones_total_cerrado_peso = exportaciones_totales_cerrado_peso['sum_peso_t']
        exportaciones_variacion_total_cerrado_peso = exportaciones_totales_cerrado_peso['diferencia_porcentual']
    else:
        exportaciones_total_cerrado = 0
        exportaciones_variacion_total_cerrado = 0
        exportaciones_total_cerrado_peso = 0
        exportaciones_variacion_total_cerrado_peso = 0
    # Corrido
    if dict_verif['exportaciones_totales_corrido'] == 'CON DATOS DE EXPORTACIONES TOTALES CORRIDO':
        exportaciones_totales_corrido = data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO']['Total'][0]
        exportaciones_total_corrido = exportaciones_totales_corrido['sum_usd_t']
        exportaciones_variacion_total_corrido = exportaciones_totales_corrido['diferencia_porcentual']
        exportaciones_totales_corrido_peso = data_dict['RESUMEN']['ST_CATEGORIAS_PESO_CORRIDO']['Total'][0]
        exportaciones_total_corrido_peso = exportaciones_totales_corrido_peso['sum_peso_t']
        exportaciones_variacion_total_corrido_peso = exportaciones_totales_corrido_peso['diferencia_porcentual']
    else:
        exportaciones_total_corrido = 0
        exportaciones_variacion_total_corrido = 0
        exportaciones_total_corrido_peso = 0   
        exportaciones_variacion_total_corrido_peso = 0

    # Cerrado NME
    if dict_verif['exportaciones_nme_cerrado'] == 'CON DATOS DE EXPORTACIONES NME CERRADO':
        exportaciones_no_minero_cerrado = data_dict['RESUMEN']['ST_CATEGORIAS_CERRADO']['No Mineras'][0]
        exportaciones_nme_cerrado = exportaciones_no_minero_cerrado['sum_usd_t']
        exportaciones_variacion_nme_cerrado = exportaciones_no_minero_cerrado['diferencia_porcentual']
        exportaciones_no_minero_cerrado_peso = data_dict['RESUMEN']['ST_CATEGORIAS_PESO_CERRADO']['No Mineras'][0]
        exportaciones_nme_cerrado_peso = exportaciones_no_minero_cerrado_peso['sum_peso_t']
        exportaciones_variacion_nme_cerrado_peso = exportaciones_no_minero_cerrado_peso['diferencia_porcentual']
    else:
        exportaciones_nme_cerrado = 0
        exportaciones_variacion_nme_cerrado = 0
        exportaciones_nme_cerrado_peso = 0
        exportaciones_variacion_nme_cerrado_peso = 0
    # Corrido NME
    if dict_verif['exportaciones_nme_corrido'] == 'CON DATOS DE EXPORTACIONES NME CORRIDO':
        exportaciones_no_minero_corrido = data_dict['RESUMEN']['ST_CATEGORIAS_CORRIDO']['No Mineras'][0]
        exportaciones_nme_corrido= exportaciones_no_minero_corrido['sum_usd_t']
        exportaciones_variacion_nme_corrido = exportaciones_no_minero_corrido['diferencia_porcentual']
        exportaciones_no_minero_corrido_peso = data_dict['RESUMEN']['ST_CATEGORIAS_PESO_CORRIDO']['No Mineras'][0]
        exportaciones_nme_corrido_peso= exportaciones_no_minero_corrido_peso['sum_peso_t']
        exportaciones_variacion_nme_corrido_peso = exportaciones_no_minero_corrido_peso['diferencia_porcentual']
        
    else:
        exportaciones_nme_corrido = 0
        exportaciones_variacion_nme_corrido = 0
        exportaciones_nme_corrido_peso = 0
        exportaciones_variacion_nme_corrido_peso = 0

    # Conteo cerrado
    if dict_verif['exportaciones_conteo_cerrado'] == 'CON DATOS DE CONTEO CERRADO':
        periodo_cerrado = data_dict['RESUMEN']['CONTEO']['CERRADO']
        num_empresas_cerrado = periodo_cerrado
    else:
        num_empresas_cerrado = 0

    # Conteo corrido
    if dict_verif['exportaciones_conteo_corrido'] == 'CON DATOS DE CONTEO CORRIDO':
        periodo_corrido = data_dict['RESUMEN']['CONTEO']['CORRIDO']
        num_empresas_corrido = periodo_corrido
    else: 
        num_empresas_corrido = 0

    # Texto por bullets de expotaciones
    if agrupacion in ['COLOMBIA', 'DEPARTAMENTOS']:
        texto_exportaciones_b1_cerrado = f"""En {export_params['cerrado']['T']}, {unidad} exportó al Mundo USD {format_number(exportaciones_total_cerrado / 1e6)} millones, {format_number(abs(exportaciones_variacion_total_cerrado))}% {variacion_palabra(exportaciones_variacion_total_cerrado)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b1_corrido = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} las exportaciones totales de {unidad} al Mundo suman USD {format_number(exportaciones_total_corrido / 1e6)} millones, {format_number(abs(exportaciones_variacion_total_corrido))}% {variacion_palabra(exportaciones_variacion_total_corrido)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b2_cerrado = f"""Las exportaciones no minero-energéticas de {unidad} al Mundo en {export_params['cerrado']['T']} registraron USD {format_number(exportaciones_nme_cerrado / 1e6)} millones, {format_number(abs(exportaciones_variacion_nme_cerrado))}% {variacion_palabra(exportaciones_variacion_nme_cerrado)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b2_corrido = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} las exportaciones no minero-energéticas de {unidad} al Mundo suman USD {format_number(exportaciones_nme_corrido / 1e6)} millones, {format_number(abs(exportaciones_variacion_nme_corrido))}% {variacion_palabra(exportaciones_variacion_nme_corrido)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b3_cerrado = f"""Durante {export_params['cerrado']['T']}, {format_number_no_decimal(num_empresas_cerrado)} empresas colombianas exportaron productos no minero-energéticos por montos superiores a USD 10.000."""
        texto_exportaciones_b3_corrido = f"""Entre entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']}, {format_number_no_decimal(num_empresas_corrido)} empresas colombianas exportaron productos no minero-energéticos por montos superiores a USD 10.000"""
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        texto_exportaciones_b1_cerrado = f"""En {export_params['cerrado']['T']}, Colombia exportó a {unidad} USD {format_number(exportaciones_total_cerrado / 1e6)} millones, {format_number(abs(exportaciones_variacion_total_cerrado))}% {variacion_palabra(exportaciones_variacion_total_cerrado)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b1_corrido = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} las exportaciones totales a {unidad} suman USD {format_number(exportaciones_total_corrido / 1e6)} millones, {format_number(abs(exportaciones_variacion_total_corrido))}% {variacion_palabra(exportaciones_variacion_total_corrido)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b2_cerrado = f"""Las exportaciones no minero-energéticas de Colombia a {unidad} en {export_params['cerrado']['T']} registraron USD {format_number(exportaciones_nme_cerrado / 1e6)} millones, {format_number(abs(exportaciones_variacion_nme_cerrado))}% {variacion_palabra(exportaciones_variacion_nme_cerrado)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b2_corrido = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} las exportaciones no minero-energéticas de Colombia a {unidad} suman USD {format_number(exportaciones_nme_corrido / 1e6)} millones, {format_number(abs(exportaciones_variacion_nme_corrido))}% {variacion_palabra(exportaciones_variacion_nme_corrido)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b3_cerrado = f"""Durante {export_params['cerrado']['T']}, {format_number_no_decimal(num_empresas_cerrado)} empresas colombianas exportaron productos no minero-energéticos a {unidad} por montos superiores a USD 10.000."""
        texto_exportaciones_b3_corrido = f"""Entre entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']}, {format_number_no_decimal(num_empresas_corrido)} empresas colombianas exportaron productos no minero-energéticos a {unidad} por montos superiores a USD 10.000."""

    # Texto por bullets de expotaciones por peso
    if agrupacion in ['COLOMBIA', 'DEPARTAMENTOS']:
        texto_exportaciones_b1_cerrado_peso = f"""En {export_params['cerrado']['T']}, {unidad} exportó al Mundo {format_number(exportaciones_total_cerrado_peso / 1000)} toneladas de productos, {format_number(abs(exportaciones_variacion_total_cerrado_peso))}% {variacion_palabra(exportaciones_variacion_total_cerrado_peso)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b1_corrido_peso = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} el peso total de las exportaciones de {unidad} al Mundo suman {format_number(exportaciones_total_corrido_peso / 1000)} toneladas, {format_number(abs(exportaciones_variacion_total_corrido_peso))}% {variacion_palabra(exportaciones_variacion_total_corrido_peso)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b2_cerrado_peso = f"""Las exportaciones no minero-energéticas de {unidad} al Mundo en {export_params['cerrado']['T']} registraron un peso de {format_number(exportaciones_nme_cerrado_peso / 1000)} toneladas, {format_number(abs(exportaciones_variacion_nme_cerrado_peso))}% {variacion_palabra(exportaciones_variacion_nme_cerrado_peso)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b2_corrido_peso = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} el peso de las exportaciones no minero-energéticas de {unidad} al Mundo suman {format_number(exportaciones_nme_corrido_peso / 1000)} toneladas, {format_number(abs(exportaciones_variacion_nme_corrido_peso))}% {variacion_palabra(exportaciones_variacion_nme_corrido_peso)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        texto_exportaciones_b1_cerrado_peso = f"""En {export_params['cerrado']['T']}, Colombia exportó a {unidad} {format_number(exportaciones_total_cerrado_peso / 1000)} toneladas, {format_number(abs(exportaciones_variacion_total_cerrado_peso))}% {variacion_palabra(exportaciones_variacion_total_cerrado_peso)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b1_corrido_peso = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} el peso total de las exportaciones a {unidad} suman {format_number(exportaciones_total_corrido_peso / 1000)} toneladas, {format_number(abs(exportaciones_variacion_total_corrido_peso))}% {variacion_palabra(exportaciones_variacion_total_corrido_peso)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
        texto_exportaciones_b2_cerrado_peso = f"""Las exportaciones no minero-energéticas de Colombia a {unidad} en {export_params['cerrado']['T']} registraron un peso {format_number(exportaciones_nme_cerrado_peso / 1000)} toneladas, {format_number(abs(exportaciones_variacion_nme_cerrado_peso))}% {variacion_palabra(exportaciones_variacion_nme_cerrado_peso)} que en {export_params['cerrado']['T_1']}."""
        texto_exportaciones_b2_corrido_peso = f"""Entre enero y {export_params['corrido']['MES_T'].lower()} de {export_params['corrido']['T_YEAR']} el peso de las exportaciones no minero-energéticas de Colombia a {unidad} suman {format_number(exportaciones_nme_corrido_peso / 1000)} toneladas, {format_number(abs(exportaciones_variacion_nme_corrido_peso))}% {variacion_palabra(exportaciones_variacion_nme_corrido_peso)} que en el mismo periodo de {export_params['corrido']['T_1_YEAR']}."""
   
    # Texto por bullets de inversión
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        # Cerrado inversión
        if dict_verif['ied_cerrado'] == 'CON DATOS DE IED CERRADO':
            # Obtener datos de IED cerrado por países o total
            if agrupacion == 'PAISES':
                inversion_total_cerrado = data_dict['RESUMEN']['IED CERRADO PAISES'][list(data_dict['RESUMEN']['IED CERRADO PAISES'].keys())[0]][0]
            else:
                inversion_total_cerrado = data_dict['RESUMEN']['IED CERRADO PAISES']['TOTAL'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            inversion_total_cerrado = {'sum_inversion_t': 0, 'diferencia_porcentual': 0}

        if dict_verif['ice_cerrado'] == 'CON DATOS DE ICE CERRADO':
            # Obtener datos de ICE cerrado por países o total
            if agrupacion == 'PAISES':
                ice_total_cerrado = data_dict['RESUMEN']['ICE CERRADO PAISES'][list(data_dict['RESUMEN']['ICE CERRADO PAISES'].keys())[0]][0]
            else:
                ice_total_cerrado = data_dict['RESUMEN']['ICE CERRADO PAISES']['TOTAL'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            ice_total_cerrado = {'sum_inversion_t': 0, 'diferencia_porcentual': 0}

        # Corrido inversión
        if dict_verif['ied_corrido'] == 'CON DATOS DE IED CORRIDO':
            # Obtener datos de IED corrido por países o total
            if agrupacion == 'PAISES':
                inversion_total_corrido = data_dict['RESUMEN']['IED CORRIDO PAISES'][list(data_dict['RESUMEN']['IED CORRIDO PAISES'].keys())[0]][0]
            else:
                inversion_total_corrido = data_dict['RESUMEN']['IED CORRIDO PAISES']['TOTAL'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            inversion_total_corrido = {'sum_inversion_t': 0, 'diferencia_porcentual': 0}

        if dict_verif['ice_corrido'] == 'CON DATOS DE ICE CORRIDO':
            # Obtener datos de ICE corrido por países o total
            if agrupacion == 'PAISES':
                ice_total_corrido = data_dict['RESUMEN']['ICE CORRIDO PAISES'][list(data_dict['RESUMEN']['ICE CORRIDO PAISES'].keys())[0]][0]
            else:
                ice_total_corrido = data_dict['RESUMEN']['ICE CORRIDO PAISES']['TOTAL'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            ice_total_corrido = {'sum_inversion_t': 0, 'diferencia_porcentual': 0}

        # Texto por bullets de inversión
        if agrupacion in ['COLOMBIA']:
            # Generar texto para agrupación por Colombia
            texto_inversion_b1_cerrado = f"""En {inversion_params['cerrado']['T']}, Colombia registró flujos {inversion_palabra(inversion_total_cerrado['sum_inversion_t'])} de inversión extranjera directa (IED) del Mundo por USD {format_number(inversion_total_cerrado['sum_inversion_t'])} millones, {format_number(abs(inversion_total_cerrado['diferencia_porcentual']))}% {variacion_palabra(inversion_total_cerrado['diferencia_porcentual'])} con respecto al {inversion_params['cerrado']['T_1']}."""
            texto_inversion_b1_corrido = f"""En el acumulado hasta el {inversion_params['corrido']['T_TRIMESTER_NAME']} trimestre de {inversion_params['corrido']['T_YEAR']}, Colombia registró flujos {inversion_palabra(inversion_total_corrido['sum_inversion_t'])} de IED del Mundo por USD {format_number(inversion_total_corrido['sum_inversion_t'])} millones, {format_number(abs(inversion_total_corrido['diferencia_porcentual']))}% {variacion_palabra(inversion_total_corrido['diferencia_porcentual'])} con respecto al mismo periodo de {inversion_params['corrido']['T_1_YEAR']}."""
            texto_inversion_b2_cerrado = f"""En {inversion_params['cerrado']['T']}, se registraron flujos {inversion_palabra(ice_total_cerrado['sum_inversion_t'])} de inversión directa de Colombia en el exterior (ICE) en el Mundo por USD {format_number(ice_total_cerrado['sum_inversion_t'])} millones, {format_number(abs(ice_total_cerrado['diferencia_porcentual']))}% {variacion_palabra(ice_total_cerrado['diferencia_porcentual'])} con respecto al {inversion_params['cerrado']['T_1']}."""
            texto_inversion_b2_corrido = f"""En el acumulado hasta el {inversion_params['corrido']['T_TRIMESTER_NAME']} trimestre de {inversion_params['corrido']['T_YEAR']}, Colombia registró flujos {inversion_palabra(ice_total_corrido['sum_inversion_t'])} de ICE en el Mundo por USD {format_number(ice_total_corrido['sum_inversion_t'])} millones, {format_number(abs(ice_total_corrido['diferencia_porcentual']))}% {variacion_palabra(ice_total_corrido['diferencia_porcentual'])} con respecto al mismo periodo de {inversion_params['corrido']['T_1_YEAR']}."""

        if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
            # Generar texto para otras agrupaciones (continentes, hubs, TLCs, países)
            texto_inversion_b1_cerrado = f"""En {inversion_params['cerrado']['T']}, Colombia registró flujos {inversion_palabra(inversion_total_cerrado['sum_inversion_t'])} de inversión extranjera directa (IED) de {unidad} por USD {format_number(inversion_total_cerrado['sum_inversion_t'])} millones, {format_number(abs(inversion_total_cerrado['diferencia_porcentual']))}% {variacion_palabra(inversion_total_cerrado['diferencia_porcentual'])} con respecto al {inversion_params['cerrado']['T_1']}."""
            texto_inversion_b1_corrido = f"""En el acumulado hasta el {inversion_params['corrido']['T_TRIMESTER_NAME']} trimestre de {inversion_params['corrido']['T_YEAR']}, Colombia registró flujos {inversion_palabra(inversion_total_corrido['sum_inversion_t'])} de IED de {unidad} por USD {format_number(inversion_total_corrido['sum_inversion_t'])} millones, {format_number(abs(inversion_total_corrido['diferencia_porcentual']))}% {variacion_palabra(inversion_total_corrido['diferencia_porcentual'])} con respecto al mismo periodo de {inversion_params['corrido']['T_1_YEAR']}."""
            texto_inversion_b2_cerrado = f"""En {inversion_params['cerrado']['T']}, se registraron flujos {inversion_palabra(ice_total_cerrado['sum_inversion_t'])} de inversión directa de Colombia en el exterior (ICE) en {unidad} por USD {format_number(ice_total_cerrado['sum_inversion_t'])} millones, {format_number(abs(ice_total_cerrado['diferencia_porcentual']))}% {variacion_palabra(ice_total_cerrado['diferencia_porcentual'])} con respecto al {inversion_params['cerrado']['T_1']}."""
            texto_inversion_b2_corrido = f"""En el acumulado hasta el {inversion_params['corrido']['T_TRIMESTER_NAME']} trimestre de {inversion_params['corrido']['T_YEAR']}, Colombia registró flujos {inversion_palabra(ice_total_corrido['sum_inversion_t'])} de ICE en {unidad} por USD {format_number(ice_total_corrido['sum_inversion_t'])} millones, {format_number(abs(ice_total_corrido['diferencia_porcentual']))}% {variacion_palabra(ice_total_corrido['diferencia_porcentual'])} con respecto al mismo periodo de {inversion_params['corrido']['T_1_YEAR']}."""

    # Texto por bullets de UNCTAD
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']: 
        # IED
        if dict_verif['ied_unctad'] == 'CON DATOS CERRADO':
            ied_unctad = data_dict['RESUMEN']['ied_unctad']
        else:
            # Llenar con cero si no hay datos disponibles
            ied_unctad = {'fdi_2022': 0, 'fdi_2023': 0}

        # ICE
        if dict_verif['ice_unctad'] == 'CON DATOS CERRADO':
            ice_unctad = data_dict['RESUMEN']['ice_unctad']
        else:
            # Llenar con cero si no hay datos disponibles
            ice_unctad = {'fdi_2022': 0, 'fdi_2023': 0}
        
    # Crear textos
    # Generar texto para UNCTAD
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']: 
        texto_unctad_ied = f"""Según datos de la UNCTAD, en 2023 {unidad} registró flujos de inversión extranjera directa (IED) proveniente del mundo por USD {format_number(ied_unctad['fdi_2023'])} millones, en comparación con 2022 que recibió USD {format_number(ied_unctad['fdi_2022'])} millones"""

    # Texo por bullets IED e ICE acumulado
    if agrupacion in ['PAISES']: 
        # IED
        if dict_verif['ied_cerrado'] == 'CON DATOS DE IED CERRADO':
            ied_acumulado = data_dict['RESUMEN']['IED PAISES ACUMULADA']['PAIS'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            ied_acumulado = {'sum_inversion_acumulada': 0, 'ranking': 0}
        
        #ICE
        if dict_verif['ice_cerrado'] == 'CON DATOS DE ICE CERRADO':
            ice_acumulado = data_dict['RESUMEN']['ICE PAISES ACUMULADA']['PAIS'][0]
        else:
            # Llenar con cero si no hay datos disponibles
            ice_acumulado = {'sum_inversion_acumulada': 0, 'ranking': 0}
        
        # Crear texto
        texto_ied_acumulado = f"""En el acumulado de datos de inversión del Banco la República de 2019 a 2023, Colombia registró flujos {inversion_palabra(ied_acumulado['sum_inversion_acumulada'])} de inversión extranjera directa (IED) de {unidad} por USD {format_number(ied_acumulado['sum_inversion_acumulada'])} millones, ubicándose como el origen número {format_number_no_decimal(ied_acumulado['ranking'])} de inversión proveniente del exterior."""
        texto_ice_acumulado = f"""Se registraron flujos {inversion_palabra(ice_acumulado['sum_inversion_acumulada'])} acmulados de 2019 a 2023 de inversión directa de Colombia en el exterior (ICE) en {unidad} por USD {format_number(ice_acumulado['sum_inversion_acumulada'])} millones, ubicándose como el destino número {format_number_no_decimal(ice_acumulado['ranking'])} de inversión hacia el exterior."""     

    
    # Obtener datos cuando existen o llenarlos con cero
    # Cerrado turismo
    if dict_verif['turismo_cerrado'] == 'CON DATOS DE TURISMO CERRADO':
        turismo_cerrado = data_dict['RESUMEN']['TURISMO CERRADO PAISES']['TOTAL'][0]
        turismo_cerrado_sum = turismo_cerrado['sum_turismo_t']
        turismo_variacion_cerrado = turismo_cerrado['diferencia_porcentual']
    else:
        turismo_cerrado_sum = 0
        turismo_variacion_cerrado = 0

    # Corrido turismo
    if dict_verif['turismo_corrido'] == 'CON DATOS DE TURISMO CORRIDO':
        turismo_corrido = data_dict['RESUMEN']['TURISMO CORRIDO PAISES']['TOTAL'][0]
        turismo_corrido_sum = turismo_corrido['sum_turismo_t']
        turismo_variacion_corrido = turismo_corrido['diferencia_porcentual']
    else:
        turismo_corrido_sum = 0
        turismo_variacion_corrido = 0
    
    # Texto por bullets de turismo
    if agrupacion in ['COLOMBIA', 'DEPARTAMENTOS']:
        texto_turismo_b1_cerrado = f"""En {turismo_params['cerrado']['T']}, {unidad} registró {format_number_no_decimal(turismo_cerrado_sum)} llegadas de turistas extranjeros, {format_number(abs(turismo_variacion_cerrado))}% {variacion_palabra(turismo_variacion_cerrado)} con respecto a {turismo_params['cerrado']['T_1']}."""
        texto_turismo_b2_corrido = f"""Entre enero y {turismo_params['corrido']['T_MONTH_NAME_FULL'].lower()} de {turismo_params['corrido']['T']}, {unidad} registró {format_number_no_decimal(turismo_corrido_sum)} llegadas de turistas extranjeros, {format_number(abs(turismo_variacion_corrido))}% {variacion_palabra(turismo_variacion_corrido)} con respecto a {turismo_params['corrido']['T_1']}."""

    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        texto_turismo_b1_cerrado = f"""En {turismo_params['cerrado']['T']}, Colombia registró {format_number_no_decimal(turismo_cerrado_sum)} llegadas de turistas extranjeros provenientes de {unidad}, {format_number(abs(turismo_variacion_cerrado))}% {variacion_palabra(turismo_variacion_cerrado)} con respecto a {turismo_params['cerrado']['T_1']}."""
        texto_turismo_b2_corrido = f"""Entre enero y {turismo_params['corrido']['T_MONTH_NAME_FULL'].lower()} de {turismo_params['corrido']['T']}, {unidad} registró {format_number_no_decimal(turismo_corrido_sum)} llegadas de turistas extranjeros provenientes de {unidad}, {format_number(abs(turismo_variacion_corrido))}% {variacion_palabra(turismo_variacion_corrido)} con respecto a {turismo_params['corrido']['T_1']}."""
    
    # Resultados
    if agrupacion in ['COLOMBIA']:
        return {
            'tab_resumen_expo': tab_resumen_expo,
            'tab_resumen_inv': tab_resumen_inv,
            'tab_resumen_tur': tab_resumen_tur,
            'texto_exportaciones_b1_cerrado' : texto_exportaciones_b1_cerrado,
            'texto_exportaciones_b1_corrido' : texto_exportaciones_b1_corrido,
            'texto_exportaciones_b2_cerrado' : texto_exportaciones_b2_cerrado,
            'texto_exportaciones_b2_corrido' : texto_exportaciones_b2_corrido,
            'texto_exportaciones_b3_cerrado' : texto_exportaciones_b3_cerrado,
            'texto_exportaciones_b3_corrido' : texto_exportaciones_b3_corrido,
            'texto_exportaciones_b1_cerrado_peso' : texto_exportaciones_b1_cerrado_peso,
            'texto_exportaciones_b1_corrido_peso' : texto_exportaciones_b1_corrido_peso,
            'texto_exportaciones_b2_cerrado_peso' : texto_exportaciones_b2_cerrado_peso,
            'texto_exportaciones_b2_corrido_peso' : texto_exportaciones_b2_corrido_peso,
            'texto_inversion_b1_cerrado' : texto_inversion_b1_cerrado,
            'texto_inversion_b1_corrido' : texto_inversion_b1_corrido,
            'texto_inversion_b2_cerrado' : texto_inversion_b2_cerrado,
            'texto_inversion_b2_corrido' : texto_inversion_b2_corrido,
            'texto_turismo_b1_cerrado' : texto_turismo_b1_cerrado,
            'texto_turismo_b2_corrido' : texto_turismo_b2_corrido
        }
    
    if agrupacion in ['CONTINENTES', 'HUBS', 'TLCS']:
        return {
            'tab_resumen_expo': tab_resumen_expo,
            'tab_resumen_inv': tab_resumen_inv,
            'tab_resumen_tur': tab_resumen_tur,
            'texto_exportaciones_b1_cerrado' : texto_exportaciones_b1_cerrado,
            'texto_exportaciones_b1_corrido' : texto_exportaciones_b1_corrido,
            'texto_exportaciones_b2_cerrado' : texto_exportaciones_b2_cerrado,
            'texto_exportaciones_b2_corrido' : texto_exportaciones_b2_corrido,
            'texto_exportaciones_b3_cerrado' : texto_exportaciones_b3_cerrado,
            'texto_exportaciones_b3_corrido' : texto_exportaciones_b3_corrido,
            'texto_exportaciones_b1_cerrado_peso' : texto_exportaciones_b1_cerrado_peso,
            'texto_exportaciones_b1_corrido_peso' : texto_exportaciones_b1_corrido_peso,
            'texto_exportaciones_b2_cerrado_peso' : texto_exportaciones_b2_cerrado_peso,
            'texto_exportaciones_b2_corrido_peso' : texto_exportaciones_b2_corrido_peso,
            'texto_inversion_b1_cerrado' : texto_inversion_b1_cerrado,
            'texto_inversion_b1_corrido' : texto_inversion_b1_corrido,
            'texto_inversion_b2_cerrado' : texto_inversion_b2_cerrado,
            'texto_inversion_b2_corrido' : texto_inversion_b2_corrido,
            'texto_turismo_b1_cerrado' : texto_turismo_b1_cerrado,
            'texto_turismo_b2_corrido' : texto_turismo_b2_corrido,
            'texto_unctad' : texto_unctad_ied
        }
    
    if agrupacion in ['PAISES']:
        return {
            'tab_resumen_expo': tab_resumen_expo,
            'tab_resumen_inv': tab_resumen_inv,
            'tab_resumen_tur': tab_resumen_tur,
            'texto_exportaciones_b1_cerrado' : texto_exportaciones_b1_cerrado,
            'texto_exportaciones_b1_corrido' : texto_exportaciones_b1_corrido,
            'texto_exportaciones_b2_cerrado' : texto_exportaciones_b2_cerrado,
            'texto_exportaciones_b2_corrido' : texto_exportaciones_b2_corrido,
            'texto_exportaciones_b3_cerrado' : texto_exportaciones_b3_cerrado,
            'texto_exportaciones_b3_corrido' : texto_exportaciones_b3_corrido,
            'texto_exportaciones_b1_cerrado_peso' : texto_exportaciones_b1_cerrado_peso,
            'texto_exportaciones_b1_corrido_peso' : texto_exportaciones_b1_corrido_peso,
            'texto_exportaciones_b2_cerrado_peso' : texto_exportaciones_b2_cerrado_peso,
            'texto_exportaciones_b2_corrido_peso' : texto_exportaciones_b2_corrido_peso,
            'texto_inversion_b1_cerrado' : texto_inversion_b1_cerrado,
            'texto_inversion_b1_corrido' : texto_inversion_b1_corrido,
            'texto_inversion_b2_cerrado' : texto_inversion_b2_cerrado,
            'texto_inversion_b2_corrido' : texto_inversion_b2_corrido,
            'texto_turismo_b1_cerrado' : texto_turismo_b1_cerrado,
            'texto_turismo_b2_corrido' : texto_turismo_b2_corrido,
            'texto_unctad' : texto_unctad_ied,
            'texto_ied_acumulado' : texto_ied_acumulado,
            'texto_ice_acumulado' : texto_ice_acumulado
        }
    
    if agrupacion in ['DEPARTAMENTOS']:
         return {
             'tab_resumen_expo': tab_resumen_expo,
             'tab_resumen_tur': tab_resumen_tur,
             'texto_exportaciones_b1_cerrado' : texto_exportaciones_b1_cerrado,
             'texto_exportaciones_b1_corrido' : texto_exportaciones_b1_corrido,
             'texto_exportaciones_b2_cerrado' : texto_exportaciones_b2_cerrado,
             'texto_exportaciones_b2_corrido' : texto_exportaciones_b2_corrido,
             'texto_exportaciones_b3_cerrado' : texto_exportaciones_b3_cerrado,
             'texto_exportaciones_b3_corrido' : texto_exportaciones_b3_corrido,
             'texto_exportaciones_b1_cerrado_peso' : texto_exportaciones_b1_cerrado_peso,
             'texto_exportaciones_b1_corrido_peso' : texto_exportaciones_b1_corrido_peso,
             'texto_exportaciones_b2_cerrado_peso' : texto_exportaciones_b2_cerrado_peso,
             'texto_exportaciones_b2_corrido_peso' : texto_exportaciones_b2_corrido_peso,
             'texto_turismo_b1_cerrado' : texto_turismo_b1_cerrado,
             'texto_turismo_b2_corrido' : texto_turismo_b2_corrido
     }
    

def crear_diccionario_cadenas(data):
    """
    Crea un diccionario donde las llaves son los valores únicos de la columna 'CADENA'
    y los valores son cadenas de los 'SUBSECTOR' correspondientes, separados por comas y terminados con un punto.

    Parámetros:
    - data (pd.DataFrame): DataFrame con las columnas 'CADENA' y 'SUBSECTOR'.

    Retorna:
    - dict: Diccionario con las cadenas y sus respectivos subsectores concatenados en una cadena.
    """

    # Crear un diccionario vacío para almacenar los resultados
    diccionario = {}

    # Verificar que las columnas necesarias existen en el DataFrame
    if 'CADENA' in data.columns and 'SUBSECTOR' in data.columns:
        # Agrupar el DataFrame por 'CADENA' y obtener los subsectores únicos
        grouped = data.groupby('CADENA')['SUBSECTOR'].unique()

        # Iterar sobre cada grupo
        for cadena, subsectores in grouped.items():
            # Filtrar valores nulos y convertir los subsectores a una lista de strings
            subsectores = [subsector for subsector in subsectores if pd.notnull(subsector)]
            # Concatenar los subsectores en una sola cadena, separados por comas y con un punto al final
            subsectores_str = ', '.join(subsectores) + '.'
            # Asignar la cadena y sus subsectores al diccionario
            diccionario[cadena] = subsectores_str
    else:
        # Si las columnas no existen, lanzar una advertencia
        print("El DataFrame no contiene las columnas 'CADENA' y 'SUBSECTOR' necesarias.")

    return diccionario


def crear_diccionario_turismo(data):
    """
    Crea un diccionario con dos cadenas de 'SECTOR' y 'SUBSECTOR' concatenados, separados por comas y terminados con un punto.

    Parámetros:
    - data (pd.DataFrame): DataFrame con las columnas 'SECTOR' y 'SUBSECTOR'.

    Retorna:
    - dict: Diccionario con la concatenación de los datos.
    """

    # Crear un diccionario vacío para almacenar los resultados
    diccionario = {}

    # Verificar que las columnas necesarias existen en el DataFrame
    if 'SECTOR' in data.columns and 'SUBSECTOR' in data.columns:
        # Obtener los sectores únicos, excluyendo valores nulos
        sectores = data['SECTOR'].dropna().unique()
        # Concatenar los sectores en una sola cadena, separados por comas y con un punto al final
        sectores_str = ', '.join(sectores) + '.'
        # Asignar al diccionario
        diccionario['SECTOR'] = sectores_str

        # Obtener los subsectores únicos, excluyendo valores nulos
        subsectores = data['SUBSECTOR'].dropna().unique()
        # Concatenar los subsectores en una sola cadena, separados por comas y con un punto al final
        subsectores_str = ', '.join(subsectores) + '.'
        # Asignar al diccionario
        diccionario['SUBSECTOR'] = subsectores_str
    else:
        # Si las columnas no existen, lanzar una advertencia
        print("El DataFrame no contiene las columnas 'SECTOR' y 'SUBSECTOR' necesarias.")

    return diccionario

def process_data(session, geo_params, dict_verificacion):
    """
    Procesa y formatea los datos obtenidos de diversas fuentes para su posterior uso en la aplicación.
    La función utiliza parámetros geográficos y de verificación para obtener y transformar datos de exportaciones,
    inversión, turismo, entre otros, renombrando columnas y aplicando formatos específicos.

    Adicionalmente, genera dos conjuntos de datos:
    - processed_data: Datos formateados para visualización general.
    - processed_data_excel: Datos formateados específicamente para exportación a Excel.

    Parámetros:
    - session: Objeto de sesión de Snowflake.
    - geo_params: Parámetros geográficos generados externamente.
    - dict_verificacion: Diccionario que indica la disponibilidad de datos para los diferentes ejes.

    Retorna:
    - processed_data (dict): Diccionario con los datos procesados y formateados para visualización general.
    - processed_data_excel (dict): Diccionario con los datos procesados y formateados para exportación a Excel.
    """
    # Extraer información básica de los parámetros geográficos
    AGRUPACION = geo_params['AGRUPACION']
    UNIDAD = geo_params['UNIDAD'][0]  # Se asume que siempre hay al menos una unidad

    #################################
    # INDICADOR DE PRESENCIA DE DATOS
    #################################

    # Verificar la disponibilidad de datos en los diferentes ejes (exportaciones, inversión, turismo)
    dict_verificacion = verif_ejes(session, geo_params)
    
    # Obtener los parámetros temporales (años y meses) para exportaciones, inversión y turismo
    params_exportaciones = get_parameters_exportaciones(session)
    params_inversion = get_parameters_inversion(session)
    params_turismo = get_parameters_turismo(session)

    # Obtener los datos crudos necesarios para el procesamiento
    data_dict = get_data(session, geo_params, dict_verificacion)

    # Diccionarios para almacenar los datos procesados y formateados
    processed_data = {}
    processed_data_excel = {}

    # Obtener DataFrames de referencia para reemplazar códigos por nombres oficiales
    df_paises_exportaciones = obtener_paises_correlativa(session, 'EXPORTACIONES')
    df_paises_inversion = obtener_paises_correlativa(session, 'INVERSION')
    df_paises_turismo = obtener_paises_correlativa(session, 'TURISMO')
    df_departamentos = obtener_departamentos_correlativa(session)
    df_municipios = obtener_municipios_correlativa(session)

    # Obtener nombres de paíse o regiones segund UNCTAD dependiendo de la agrupación activa
    if AGRUPACION in ['CONTINENTES']:
        df_paises_unctad = obtener_paises_correlativa(session, 'UNCTAD_CONTINENTE')
    elif AGRUPACION in ['HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        df_paises_unctad = obtener_paises_correlativa(session, 'UNCTAD_PAIS')

    ###############
    # Exportaciones
    ###############

    # Diccionario para renombrar las columnas de categorías en exportaciones
    column_names_dict_exportaciones = {
        'TIPO': 'Tipo de exportación',
        'CONTINENTE': 'Continente',
        'DEPARTAMENTOS': 'Departamento de origen',
        'HUBs': 'HUB',
        'PAIS': 'País destino',
        'SECTORES': 'Sector',
        'SUBSECTORES': 'Subsector',
        'TLCS': 'Tratados de Libre Comercio'
    }

    # Función auxiliar para procesar y formatear DataFrames de exportaciones
    def process_exportaciones(data_section_keys, params):
        """
        Procesa y formatea los datos de exportaciones para las secciones especificadas.

        Parámetros:
        - data_section_keys (list): Lista de claves que representan las secciones a procesar.
        - params (dict): Parámetros temporales para renombrar columnas.

        Retorna:
        - processed_sub_data (dict): Datos procesados y formateados para visualización general.
        - processed_sub_data_excel (dict): Datos procesados y formateados para exportación a Excel.
        """
        processed_sub_data = {}
        processed_sub_data_excel = {}

        for key in data_section_keys:
            sub_dict = data_dict.get(key, {})
            processed_sub_dict = {}
            processed_sub_dict_excel = {}

            for sub_key, df in sub_dict.items():
                if df.empty:
                    continue  # Saltar DataFrames vacíos

                # Reemplazar códigos de país por nombres oficiales si corresponde
                if sub_key == 'PAIS':
                    df = df.merge(
                        df_paises_exportaciones[['PAIS_LLAVE_EXPORTACIONES', 'COUNTRY_OR_AREA_UNSD']],
                        left_on='CATEGORIA',
                        right_on='PAIS_LLAVE_EXPORTACIONES',
                        how='left'
                    )
                    df['CATEGORIA'] = df['COUNTRY_OR_AREA_UNSD'].combine_first(df['CATEGORIA'])
                    df.drop(['PAIS_LLAVE_EXPORTACIONES', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)

                # Renombrar la columna 'CATEGORIA' según el diccionario de nombres
                if 'CATEGORIA' in df.columns and sub_key in column_names_dict_exportaciones:
                    df.rename(columns={'CATEGORIA': column_names_dict_exportaciones[sub_key]}, inplace=True)

                # Renombrar y formatear columnas de valores y participaciones
                if {'SUMA_USD_T_1', 'SUMA_USD_T'}.issubset(df.columns):
                    df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
                    if 'CERRADO' in key:
                        df.rename(columns={
                            'SUMA_USD_T_1': f"{params['cerrado']['T_1']} (USD FOB)",
                            'SUMA_USD_T': f"{params['cerrado']['T']} (USD FOB)",
                            'PARTICIPACION_T': f"Participación (%) {transform_year_column_name(params['cerrado']['T'])}"
                        }, inplace=True)
                    else:  # 'CORRIDO' in key
                        df.rename(columns={
                            'SUMA_USD_T_1': f"{transform_year_column_name(params['corrido']['T_1'])} (USD FOB)",
                            'SUMA_USD_T': f"{transform_year_column_name(params['corrido']['T'])} (USD FOB)",
                            'PARTICIPACION_T': f"Participación (%) {transform_year_column_name(params['corrido']['T'])}"
                        }, inplace=True)

                # Crear copias para aplicar diferentes formatos
                df_formatted = df.copy()
                df_excel = df.copy()

                # Aplicar formato a las columnas numéricas y porcentuales
                df_formatted = format_columns_exportaciones(df_formatted)
                df_excel = format_columns_exportaciones_excel(df_excel)

                processed_sub_dict[sub_key] = df_formatted
                processed_sub_dict_excel[sub_key] = df_excel

            processed_sub_data[key] = processed_sub_dict
            processed_sub_data_excel[key] = processed_sub_dict_excel

        return processed_sub_data, processed_sub_data_excel

    # Procesar los datos de exportaciones para las secciones especificadas
    export_processed_data, export_processed_data_excel = process_exportaciones(['CATEGORIAS CERRADO', 'CATEGORIAS CORRIDO'], params_exportaciones)
    processed_data.update(export_processed_data)
    processed_data_excel.update(export_processed_data_excel)

    # Procesar y formatear los datos de 'TIPOS' y 'TIPOS PESO' en exportaciones
    def process_tipos_exportaciones(keys_list, params):
        """
        Procesa y formatea los datos de 'TIPOS' y 'TIPOS PESO' en exportaciones.

        Parámetros:
        - keys_list (list): Lista de claves que representan las secciones a procesar.
        - params (dict): Parámetros temporales para renombrar columnas.

        Retorna:
        - processed_sub_data (dict): Datos procesados y formateados para visualización general.
        - processed_sub_data_excel (dict): Datos procesados y formateados para exportación a Excel.
        """
        processed_sub_data = {}
        processed_sub_data_excel = {}

        for key in keys_list:
            sub_dict = data_dict.get(key, {})
            processed_sub_dict = {}
            processed_sub_dict_excel = {}

            for sub_key, df in sub_dict.items():
                if df.empty:
                    continue

                # Renombrar la columna 'CATEGORIA' a 'Tipo de exportación'
                if 'CATEGORIA' in df.columns:
                    df.rename(columns={'CATEGORIA': 'Tipo de exportación'}, inplace=True)

                # Renombrar y formatear columnas de valores y participaciones
                if {'SUMA_USD_T_1', 'SUMA_USD_T'}.issubset(df.columns) or {'SUMA_PESO_T_1', 'SUMA_PESO_T'}.issubset(df.columns):
                    df.rename(columns={'DIFERENCIA_PORCENTUAL': 'Variación (%)'}, inplace=True)
                    if 'ST_CATEGORIAS_CERRADO' in sub_key or 'ST_CATEGORIAS_PESO_CERRADO' in sub_key:
                        time_params = params['cerrado']
                    else:
                        time_params = params['corrido']

                    # Renombrar columnas según si son valores en USD o peso en toneladas
                    if 'SUMA_USD_T_1' in df.columns:
                        df.rename(columns={
                            'SUMA_USD_T_1': f"{transform_year_column_name(time_params['T_1'])} (USD FOB)",
                            'SUMA_USD_T': f"{transform_year_column_name(time_params['T'])} (USD FOB)",
                            'PARTICIPACION_T': f"Participación (%) {transform_year_column_name(time_params['T'])}"
                        }, inplace=True)
                    elif 'SUMA_PESO_T_1' in df.columns:
                        df['SUMA_PESO_T_1'] /= 1000  # Convertir a toneladas
                        df['SUMA_PESO_T'] /= 1000
                        df.rename(columns={
                            'SUMA_PESO_T_1': f"{transform_year_column_name(time_params['T_1'])} (TONELADAS)",
                            'SUMA_PESO_T': f"{transform_year_column_name(time_params['T'])} (TONELADAS)",
                            'PARTICIPACION_T': f"Participación (%) {transform_year_column_name(time_params['T'])}"
                        }, inplace=True)

                # Crear copias para aplicar diferentes formatos
                df_formatted = df.copy()
                df_excel = df.copy()

                # Aplicar formato a las columnas
                df_formatted = format_columns_exportaciones(df_formatted)
                df_excel = format_columns_exportaciones_excel(df_excel)

                processed_sub_dict[sub_key] = df_formatted
                processed_sub_dict_excel[sub_key] = df_excel

            processed_sub_data[key] = processed_sub_dict
            processed_sub_data_excel[key] = processed_sub_dict_excel

        return processed_sub_data, processed_sub_data_excel

    # Procesar y formatear los datos de 'TIPOS' y 'TIPOS PESO'
    tipos_processed_data, tipos_processed_data_excel = process_tipos_exportaciones(['TIPOS', 'TIPOS PESO', 'MEDIOS PESO MINERO', 'MEDIOS PESO NO MINERO'], params_exportaciones)
    processed_data.update(tipos_processed_data)
    processed_data_excel.update(tipos_processed_data_excel)

    # Procesar y formatear los datos de 'EMPRESAS' en exportaciones
    def process_empresas_exportaciones(key, params):
        """
        Procesa y formatea los datos de 'EMPRESAS' en exportaciones.

        Parámetros:
        - key (str): Clave que representa la sección a procesar.
        - params (dict): Parámetros temporales para renombrar columnas.

        Retorna:
        - processed_sub_data (dict): Datos procesados y formateados para visualización general.
        - processed_sub_data_excel (dict): Datos procesados y formateados para exportación a Excel.
        """
        sub_dict = data_dict.get(key, {})
        processed_sub_dict = {}
        processed_sub_dict_excel = {}

        for sub_key, df in sub_dict.items():
            if df.empty:
                continue

            # Renombrar columnas relevantes
            df.rename(columns={
                'CATEGORIA': 'NIT',
                'DIFERENCIA_PORCENTUAL': 'Variación (%)',
                'RAZON_SOCIAL': 'Razón Social',
                'SECTOR_ESTRELLA': 'Sector'
            }, inplace=True)

            # Renombrar columnas de valores según el período
            if 'SUMA_USD_T_1' in df.columns:
                if 'CERRADO' in sub_key:
                    time_params = params['cerrado']
                else:
                    time_params = params['corrido']
                df.rename(columns={
                    'SUMA_USD_T_1': f"{transform_year_column_name(time_params['T_1'])} (USD FOB)",
                    'SUMA_USD_T': f"{transform_year_column_name(time_params['T'])} (USD FOB)",
                    'PARTICIPACION_T': f"Participación (%) {transform_year_column_name(time_params['T'])}"
                }, inplace=True)

            # Crear copias para aplicar diferentes formatos
            df_formatted = df.copy()
            df_excel = df.copy()

            # Aplicar formato a las columnas
            df_formatted = format_columns_exportaciones(df_formatted)
            df_excel = format_columns_exportaciones_excel(df_excel)

            processed_sub_dict[sub_key] = df_formatted
            processed_sub_dict_excel[sub_key] = df_excel

        return processed_sub_dict, processed_sub_dict_excel

    empresas_processed_data, empresas_processed_data_excel = process_empresas_exportaciones('EMPRESAS', params_exportaciones)
    processed_data['EMPRESAS'] = empresas_processed_data
    processed_data_excel['EMPRESAS'] = empresas_processed_data_excel

    # Agregar 'CONTEO EMPRESAS' sin cambios
    processed_data['CONTEO EMPRESAS'] = data_dict.get('CONTEO EMPRESAS', 0)
    processed_data_excel['CONTEO EMPRESAS'] = data_dict.get('CONTEO EMPRESAS', 0)

    ###########
    # Inversión
    ###########

    # Diccionario para renombrar las columnas en inversión
    if AGRUPACION == 'PAISES':
        column_names_dict_inversion = {
        'IED ACTIVIDADES COLOMBIA': 'Actividad económica',
        'IED PAISES': 'País',
        'ICE PAISES': 'País',
        'IED TOTAL': 'País',
        'ICE TOTAL': 'País'
        }
    else:
        column_names_dict_inversion = {
        'IED ACTIVIDADES COLOMBIA': 'Actividad económica',
        'IED PAISES': 'País',
        'ICE PAISES': 'País',
        'IED TOTAL': 'Agrupación de países',
        'ICE TOTAL': 'Agrupación de países'
        }    

    # Función auxiliar para procesar y formatear DataFrames de inversión
    def process_inversion(keys_list):
        """
        Procesa y formatea los datos de inversión para las secciones especificadas.

        Parámetros:
        - keys_list (list): Lista de claves que representan las secciones a procesar.

        Retorna:
        - processed_sub_data (dict): Datos procesados y formateados para visualización general.
        - processed_sub_data_excel (dict): Datos procesados y formateados para exportación a Excel.
        """
        processed_sub_data = {}
        processed_sub_data_excel = {}

        for key in keys_list:
            sub_dict = data_dict.get(key, {})
            processed_sub_dict = {}
            processed_sub_dict_excel = {}

            for sub_key, df in sub_dict.items():
                if df.empty:
                    continue

                # Reemplazar códigos de país por nombres oficiales si corresponde
                if key in ['IED PAISES', 'ICE PAISES']:
                    df = df.merge(
                        df_paises_inversion[['PAIS_INVERSION_BANREP', 'COUNTRY_OR_AREA_UNSD']],
                        left_on='UNIDAD',
                        right_on='PAIS_INVERSION_BANREP',
                        how='left'
                    )
                    df['UNIDAD'] = df['COUNTRY_OR_AREA_UNSD'].combine_first(df['UNIDAD'])
                    df.drop(['PAIS_INVERSION_BANREP', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)

                # Renombrar la columna 'UNIDAD' según el diccionario de nombres
                if 'UNIDAD' in df.columns and key in column_names_dict_inversion:
                    df.rename(columns={'UNIDAD': column_names_dict_inversion[key]}, inplace=True)

                # Renombrar y formatear columnas de valores y participaciones
                if 'SUMA_INVERSION_T_1' in df.columns:
                    if 'cerrado' in sub_key:
                        time_params = params_inversion['cerrado']
                    else:
                        time_params = params_inversion['corrido']
                    df.rename(columns={
                        'SUMA_INVERSION_T_1': f"{transform_year_column_name(time_params['T_1'])} (USD millones)",
                        'SUMA_INVERSION_T': f"{transform_year_column_name(time_params['T'])} (USD millones)",
                        'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {transform_year_column_name(time_params['T'])}",
                        'DIFERENCIA_PORCENTUAL': 'Variación (%)',
                        'PARTICIPACION_T': f"Participación (%) {transform_year_column_name(time_params['T'])}"
                    }, inplace=True)

                # Crear copias para aplicar diferentes formatos
                df_formatted = df.copy()
                df_excel = df.copy()

                # Aplicar formato a las columnas
                df_formatted = format_columns_inversion(df_formatted)
                df_excel = format_columns_inversion_excel(df_excel)

                processed_sub_dict[sub_key] = df_formatted
                processed_sub_dict_excel[sub_key] = df_excel

            processed_sub_data[key] = processed_sub_dict
            processed_sub_data_excel[key] = processed_sub_dict_excel

        return processed_sub_data, processed_sub_data_excel

    # Procesar los datos de inversión según la agrupación
    inversion_keys = []
    if AGRUPACION == 'COLOMBIA':
        inversion_keys.append('IED ACTIVIDADES COLOMBIA')
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'COLOMBIA']:
        inversion_keys.extend(['IED PAISES', 'ICE PAISES'])
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES']:
        inversion_keys.extend(['IED TOTAL', 'ICE TOTAL'])

    inversion_processed_data, inversion_processed_data_excel = process_inversion(inversion_keys)
    processed_data.update(inversion_processed_data)
    processed_data_excel.update(inversion_processed_data_excel)

    ########
    # UNCTAD
    ########
    keys_unctad = ['UNCTAD']

    # Procesar para casos en que esté
    for key in keys_unctad:
        sub_dict_unctad = data_dict.get(key, {})
        processed_sub_data_unctad = {}
        processed_sub_data_excel_unctad = {}

        for sub_key, df in sub_dict_unctad.items():  # Aquí sub_key puede ser 'ied_unctad' o 'ice_unctad'
            if df.empty:
                continue  # Saltar DataFrames vacíos
            
            # Reemplazar códigos de país por nombres oficiales si corresponde
            df = df.merge(
                df_paises_unctad[['M49_CODE', 'COUNTRY_OR_AREA_UNSD']],
                left_on='ECONOMY',
                right_on='M49_CODE',
                how='left'
            )
            df['ECONOMY'] = df['COUNTRY_OR_AREA_UNSD'].combine_first(df['ECONOMY'])
            df.drop(['M49_CODE', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)
            
            # Renombrar economía
            df.rename(columns={'ECONOMY': 'País'}, inplace=True)
            
            # Crear copias para aplicar diferentes formatos
            df_formatted_unctad = df.copy()
            df_excel_unctad = df.copy()

            # Aplicar formato a las columnas numéricas
            df_formatted_unctad = format_columns_unctad(df_formatted_unctad)
            df_excel_unctad = format_columns_unctad_excel(df_excel_unctad)

            # Agregar al subdiccionario de resultados
            processed_sub_data_unctad[sub_key] = df_formatted_unctad
            processed_sub_data_excel_unctad[sub_key] = df_excel_unctad
        
        # Agregar al diccionario de resultado de la llave UNCTAD
        processed_data[key] = processed_sub_data_unctad
        processed_data_excel[key] = processed_sub_data_excel_unctad

    #########
    # BALANZA
    #########

    keys_balanza = ['BALANZA']

    for key in keys_balanza:
        sub_dict_balanza = data_dict.get(key, {})
        processed_sub_data_balanza = {}
        processed_sub_data_excel_balanza = {}

        for sub_key, df in sub_dict_balanza.items():
            
            if df.empty:
                continue  # Saltar DataFrames vacíos
            
            # Cambiar nombre
            df.rename(columns={'TIPO': 'Tipo'}, inplace=True)

            # Crear copias para aplicar diferentes formatos
            df_formatted_balanza = df.copy()
            df_excel_balanza = df.copy()

            # Aplicar formato a las columnas numéricas
            df_formatted_balanza = format_columns_balanza(df_formatted_balanza)
            df_excel_balanza = format_columns_balanza_excel(df_excel_balanza)

            # Agregar al subdiccionario de resultados
            processed_sub_data_balanza[sub_key] = df_formatted_balanza
            processed_sub_data_excel_balanza[sub_key] = df_excel_balanza
        
        # Agregar al diccionario de resultado de la llave Balanza
        processed_data[key] = processed_sub_data_balanza
        processed_data_excel[key] = processed_sub_data_excel_balanza

    #########
    # Turismo
    #########

    # Diccionario para renombrar las columnas en turismo
    column_names_dict_turismo = {
        'PAIS_RESIDENCIA': 'País de residencia',
        'DPTO_HOSPEDAJE': 'Departamento de hospedaje',
        'CIUDAD_HOSPEDAJE': 'Ciudad de hospedaje',
        'DESCRIPCION_GENERO': 'Género',
        'MOVC_NOMBRE': 'Motivo de viaje'
    }

    # Función auxiliar para procesar y formatear DataFrames de turismo
    def process_turismo(key):
        """
        Procesa y formatea los datos de turismo para la sección especificada.

        Parámetros:
        - key (str): Clave que representa la sección a procesar ('TURISMO CERRADO' o 'TURISMO CORRIDO').

        Retorna:
        - processed_sub_data (dict): Datos procesados y formateados para visualización general.
        - processed_sub_data_excel (dict): Datos procesados y formateados para exportación a Excel.
        """
        sub_dict = data_dict.get(key, {})
        processed_sub_dict = {}
        processed_sub_dict_excel = {}

        for sub_key, df in sub_dict.items():
            if df.empty:
                continue

            # Renombrar la columna principal según el diccionario de nombres
            if sub_key in column_names_dict_turismo:
                df.rename(columns={df.columns[0]: column_names_dict_turismo[sub_key]}, inplace=True)

            # Reemplazar códigos por nombres oficiales si corresponde
            if sub_key == 'PAIS_RESIDENCIA':
                df = df.merge(
                    df_paises_turismo[['CODIGO_PAIS_MIGRACION', 'COUNTRY_OR_AREA_UNSD']],
                    left_on='País de residencia',
                    right_on='CODIGO_PAIS_MIGRACION',
                    how='left'
                )
                df['País de residencia'] = df['COUNTRY_OR_AREA_UNSD'].combine_first(df['País de residencia'])
                df.drop(['CODIGO_PAIS_MIGRACION', 'COUNTRY_OR_AREA_UNSD'], axis=1, inplace=True)

            elif sub_key == 'DPTO_HOSPEDAJE':
                df = df.merge(
                    df_departamentos[['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN']],
                    left_on='Departamento de hospedaje',
                    right_on='COD_DIAN_DEPARTAMENTO',
                    how='left'
                )
                df['Departamento de hospedaje'] = df['DEPARTAMENTO_DIAN'].combine_first(df['Departamento de hospedaje'])
                df.drop(['COD_DIAN_DEPARTAMENTO', 'DEPARTAMENTO_DIAN'], axis=1, inplace=True)

            elif sub_key == 'CIUDAD_HOSPEDAJE':
                df = df.merge(
                    df_municipios[['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE']],
                    left_on='Ciudad de hospedaje',
                    right_on='COD_DANE_MUNICIPIO',
                    how='left'
                )
                df['Ciudad de hospedaje'] = df['MUNICIPIO_DANE'].combine_first(df['Ciudad de hospedaje'])
                df.drop(['COD_DANE_MUNICIPIO', 'MUNICIPIO_DANE'], axis=1, inplace=True)

            # Renombrar y formatear columnas de valores y participaciones
            if 'SUMA_TURISMO_T_1' in df.columns:
                if 'CERRADO' in key:
                    time_params = params_turismo['cerrado']
                    df.rename(columns={
                        'SUMA_TURISMO_T_1': f"{time_params['T_1']}",
                        'SUMA_TURISMO_T': f"{time_params['T']}",
                        'DIFERENCIA_PORCENTUAL_T': f"Variación (%) {time_params['T']}",
                        'PARTICIPACION_T': f"Participación (%) {time_params['T']}"
                    }, inplace=True)
                else:  # 'CORRIDO' in key
                    time_params = params_turismo['corrido']
                    df.rename(columns={
                        'SUMA_TURISMO_T_1': f"Ene - {time_params['T_MONTH_NAME']} {time_params['T_1']}",
                        'SUMA_TURISMO_T': f"Ene - {time_params['T_MONTH_NAME']} {time_params['T']}",
                        'DIFERENCIA_ABSOLUTA': 'Diferencia (turistas)',
                        'DIFERENCIA_PORCENTUAL': f"Variación (%) Ene - {time_params['T_MONTH_NAME']} {time_params['T']}",
                        'PARTICIPACION_T': f"Participación (%) Ene - {time_params['T_MONTH_NAME']} {time_params['T']}"
                    }, inplace=True)

            # Crear copias para aplicar diferentes formatos
            df_formatted = df.copy()
            df_excel = df.copy()

            # Aplicar formato a las columnas
            df_formatted = format_columns_turismo(df_formatted)
            df_excel = format_columns_turismo_excel(df_excel)

            processed_sub_dict[sub_key] = df_formatted
            processed_sub_dict_excel[sub_key] = df_excel

        return processed_sub_dict, processed_sub_dict_excel

    # Procesar y formatear los datos de turismo para 'TURISMO CERRADO' y 'TURISMO CORRIDO'
    turismo_cerrado_data, turismo_cerrado_excel = process_turismo('TURISMO CERRADO')
    processed_data['TURISMO CERRADO'] = turismo_cerrado_data
    processed_data_excel['TURISMO CERRADO'] = turismo_cerrado_excel

    turismo_corrido_data, turismo_corrido_excel = process_turismo('TURISMO CORRIDO')
    processed_data['TURISMO CORRIDO'] = turismo_corrido_data
    processed_data_excel['TURISMO CORRIDO'] = turismo_corrido_excel

    #################
    # Hoja de resumen
    #################

    # Generar el resumen de datos según la agrupación
    if AGRUPACION == 'COLOMBIA':
        resumen = resumen_datos(data_dict, AGRUPACION, 'Colombia', params_exportaciones, params_inversion, params_turismo, dict_verificacion)
    elif AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'DEPARTAMENTOS']:
        resumen = resumen_datos(data_dict, AGRUPACION, UNIDAD, params_exportaciones, params_inversion, params_turismo, dict_verificacion)
    elif AGRUPACION == 'PAISES':
        nombre_pais = geo_params['NOMBRE PAIS'][0]
        resumen = resumen_datos(data_dict, AGRUPACION, nombre_pais, params_exportaciones, params_inversion, params_turismo, dict_verificacion)
    else:
        resumen = {}

    # Agregar el resumen al diccionario de datos procesados
    processed_data['RESUMEN'] = resumen
    processed_data_excel['RESUMEN'] = resumen  # Se asume que el resumen es el mismo para ambos

    ##############
    # Conectividad
    ##############

    # Agregar datos de conectividad si están disponibles y la agrupación es 'DEPARTAMENTOS'
    if AGRUPACION == 'DEPARTAMENTOS' and dict_verificacion.get('conectividad') == "CON DATOS DE CONECTIVIDAD":
        processed_data['CONECTIVIDAD'] = data_dict.get('CONECTIVIDAD', {})
        processed_data_excel['CONECTIVIDAD'] = data_dict.get('CONECTIVIDAD', {})

    ###############
    # Oportunidades
    ###############

    # Procesar oportunidades en exportación, inversión y turismo si están disponibles
    if AGRUPACION in ['CONTINENTES', 'HUBS', 'TLCS', 'PAISES', 'DEPARTAMENTOS', 'COLOMBIA']:
        if dict_verificacion.get('oportunidades_exportacion') == "CON OPORTUNIDADES":
            oportunidades_export = crear_diccionario_cadenas(data_dict['OPORTUNIDADES']['EXPORTACIONES'])
            processed_data['OPORTUNIDADES_EXPORTACIONES'] = oportunidades_export
            processed_data_excel['OPORTUNIDADES_EXPORTACIONES'] = oportunidades_export

        if dict_verificacion.get('oportunidades_inversion') == "CON OPORTUNIDADES":
            oportunidades_inversion = crear_diccionario_cadenas(data_dict['OPORTUNIDADES']['INVERSION'])
            processed_data['OPORTUNIDADES_INVERSION'] = oportunidades_inversion
            processed_data_excel['OPORTUNIDADES_INVERSION'] = oportunidades_inversion

        if dict_verificacion.get('oportunidades_turismo') == "CON OPORTUNIDADES":
            oportunidades_turismo = crear_diccionario_turismo(data_dict['OPORTUNIDADES']['TURISMO'])
            processed_data['TURISMO_PRINCIPAL'] = oportunidades_turismo.get('SECTOR', '')
            processed_data['TURISMO_NICHOS'] = oportunidades_turismo.get('SUBSECTOR', '')
            processed_data_excel['TURISMO_PRINCIPAL'] = oportunidades_turismo.get('SECTOR', '')
            processed_data_excel['TURISMO_NICHOS'] = oportunidades_turismo.get('SUBSECTOR', '')

    #################################
    # Peso por medios de transporte
    #################################

    # Procesar y formatear datos de peso por medios de transporte
    def process_peso_medios(keys_list):
        """
        Procesa y formatea los datos de peso por medios de transporte.

        Parámetros:
        - keys_list (list): Lista de claves que representan las secciones a procesar.

        Retorna:
        - processed_sub_data (dict): Datos procesados y formateados para visualización general.
        - processed_sub_data_excel (dict): Datos procesados y formateados para exportación a Excel.
        """
        processed_sub_data = {}
        processed_sub_data_excel = {}

        for key in keys_list:
            sub_dict = data_dict.get(key, {})
            processed_sub_dict = {}
            processed_sub_dict_excel = {}

            for sub_key, df in sub_dict.items():
                if df.empty:
                    continue

                # Renombrar la columna 'CATEGORIA' a 'Medio de transporte'
                if 'CATEGORIA' in df.columns:
                    df.rename(columns={'CATEGORIA': 'Medio de transporte'}, inplace=True)

                # Convertir peso a toneladas y renombrar columnas
                if {'SUMA_PESO_T_1', 'SUMA_PESO_T'}.issubset(df.columns):
                    df['SUMA_PESO_T_1'] /= 1000  # Convertir a toneladas
                    df['SUMA_PESO_T'] /= 1000
                    if 'CERRADO' in sub_key:
                        time_params = params_exportaciones['cerrado']
                    else:
                        time_params = params_exportaciones['corrido']
                    df.rename(columns={
                        'SUMA_PESO_T_1': f"{transform_year_column_name(time_params['T_1'])} (TONELADAS)",
                        'SUMA_PESO_T': f"{transform_year_column_name(time_params['T'])} (TONELADAS)",
                        'PARTICIPACION_T': f"Participación (%) {transform_year_column_name(time_params['T'])}",
                        'PARTICIPACION_T_1': f"Participación (%) {transform_year_column_name(time_params['T_1'])}"
                    }, inplace=True)

                # Crear copias para aplicar diferentes formatos
                df_formatted = df.copy()
                df_excel = df.copy()

                # Aplicar formato a las columnas
                df_formatted = format_columns_exportaciones(df_formatted)
                df_excel = format_columns_exportaciones_excel(df_excel)

                processed_sub_dict[sub_key] = df_formatted
                processed_sub_dict_excel[sub_key] = df_excel

            processed_sub_data[key] = processed_sub_dict
            processed_sub_data_excel[key] = processed_sub_dict_excel

        return processed_sub_data, processed_sub_data_excel

    peso_medios_processed_data, peso_medios_processed_data_excel = process_peso_medios(['MEDIOS PESO MINERO', 'MEDIOS PESO NO MINERO'])
    processed_data.update(peso_medios_processed_data)
    processed_data_excel.update(peso_medios_processed_data_excel)

    # Retornar los diccionarios con todos los datos procesados y formateados
    return processed_data, processed_data_excel

def guardar_tablas_en_excel(data_dict, file_path):
    """
    Guarda los DataFrames contenidos en data_dict en un objeto BytesIO como archivo Excel, organizándolos en pestañas específicas
    según un mapeo predefinido. El parámetro data_dict es el resultado de la función process_data() con formato Excel.

    Parámetros:
    - data_dict (dict): Diccionario que contiene los DataFrames a guardar. Estructura obtenida de process_data() con formato Excel.
    - file_path (BytesIO): Objeto BytesIO donde se guardarán los datos en formato Excel.

    Retorna:
    - None: La función escribe el archivo Excel en el objeto BytesIO proporcionado.
    """

    # Diccionario que mapea las combinaciones de claves en data_dict a nombres específicos de pestañas en Excel
    sheet_name_mapping = {
        # EXPORTACIONES TOTALES
        ('TOTALES', 'ST_CATEGORIAS_CERRADO'): 'EXPO_TOTAL_CERRADO',
        ('TOTALES', 'ST_CATEGORIAS_CORRIDO'): 'EXPO_TOTAL_CORRIDO',
        # EXPORTACIONES TOTALES POR TIPO
        ('TIPOS', 'ST_CATEGORIAS_CERRADO'): 'EXPO_TIPOS_CERRADO',
        ('TIPOS', 'ST_CATEGORIAS_CORRIDO'): 'EXPO_TIPOS_CORRIDO',
        # EXPORTACIONES NME POR VARIABLE AÑO CERRADO
        ('CATEGORIAS CERRADO', 'CONTINENTE'): 'EXPO_CONTINENTE_CERRADO',
        ('CATEGORIAS CERRADO', 'DEPARTAMENTOS'): 'EXPO_DPTO_CERRADO',
        ('CATEGORIAS CERRADO', 'HUBS'): 'EXPO_HUB_CERRADO',
        ('CATEGORIAS CERRADO', 'PAIS'): 'EXPO_PAIS_CERRADO',
        ('CATEGORIAS CERRADO', 'SECTORES'): 'EXPO_SECTOR_CERRADO',
        ('CATEGORIAS CERRADO', 'SUBSECTORES'): 'EXPO_SUBSECTOR_CERRADO',
        ('CATEGORIAS CERRADO', 'TLCS'): 'EXPO_TLC_CERRADO',
        # EXPORTACIONES NME POR VARIABLE AÑO CORRIDO
        ('CATEGORIAS CORRIDO', 'CONTINENTE'): 'EXPO_CONTINENTE_CORRIDO',
        ('CATEGORIAS CORRIDO', 'DEPARTAMENTOS'): 'EXPO_DPTO_CORRIDO',
        ('CATEGORIAS CORRIDO', 'HUBS'): 'EXPO_HUB_CORRIDO',
        ('CATEGORIAS CORRIDO', 'PAIS'): 'EXPO_PAIS_CORRIDO',
        ('CATEGORIAS CORRIDO', 'SECTORES'): 'EXPO_SECTOR_CORRIDO',
        ('CATEGORIAS CORRIDO', 'SUBSECTORES'): 'EXPO_SUBSECTOR_CORRIDO',
        ('CATEGORIAS CORRIDO', 'TLCS'): 'EXPO_TLC_CORRIDO',
        # DATOS DE EMPRESAS NME POR AÑO CERRADO
        ('EMPRESAS', 'ST_NIT_CERRADO'): 'NIT_CERRADO',
        # DATOS DE EMPRESAS NME POR AÑO CORRIDO
        ('EMPRESAS', 'ST_NIT_CORRIDO'): 'NIT_CORRIDO',
        # CONTEO DE EMPRESAS AÑO CERRADO
        ('CONTEO EMPRESAS', 'CERRADO'): 'CONTEO_CERRADO',
        # CONTEO DE EMPRESAS AÑO CORRIDO
        ('CONTEO EMPRESAS', 'CORRIDO'): 'CONTEO_CORRIDO',
        # EXPORTACIONES TOTALES POR PESO AÑO CERRADO
        ('TOTALES PESO', 'ST_CATEGORIAS_PESO_CERRADO'): 'EXPO_PESO_CERRADO',
        # EXPORTACIONES TOTALES POR PESO AÑO CORRIDO
        ('TOTALES PESO', 'ST_CATEGORIAS_PESO_CORRIDO'): 'EXPO_PESO_CORRIDO',
        # EXPORTACIONES POR TIPO POR PESO AÑO CERRADO
        ('TIPOS PESO', 'ST_CATEGORIAS_PESO_CERRADO'): 'EXPO_TIPOS_PESO_CERRADO',
        # EXPORTACIONES POR TIPO POR PESO AÑO CORRIDO
        ('TIPOS PESO', 'ST_CATEGORIAS_PESO_CORRIDO'): 'EXPO_TIPOS_PESO_CORRIDO',
        # BALANZA COMERCIAL
        ('BALANZA', 'BALANZA'): 'BALANZA_COMERCIAL',
        # IED ACTIVIDADES AÑO CERRADO
        ('IED ACTIVIDADES COLOMBIA', 'ied_cerrado'): 'IED_ACTIVIDADES_CERRADO',
        # IED ACTIVIDADES AÑO CORRIDO
        ('IED ACTIVIDADES COLOMBIA', 'ied_corrido'): 'IED_ACTIVIDADES_CORRIDO',
        # IED PAISES AÑO CERRADO
        ('IED PAISES', 'ied_cerrado'): 'IED_PAISES_CERRADO',
        # IED PAISES AÑO CORRIDO
        ('IED PAISES', 'ied_corrido'): 'IED_PAISES_CORRIDO',
        # IED TOTAL
        ('IED TOTAL', ''): 'IED_TOTAL',
        # ICE PAISES AÑO CERRADO
        ('ICE PAISES', 'ice_cerrado'): 'ICE_PAISES_CERRADO',
        # ICE PAISES AÑO CORRIDO
        ('ICE PAISES', 'ice_corrido'): 'ICE_PAISES_CORRIDO',
        # ICE TOTAL
        ('ICE TOTAL', ''): 'ICE_TOTAL',
        # IED UNCTAD
        ('UNCTAD', 'ied_unctad'): 'IED_UNCTAD',
        # ICE UNCTAD
        ('UNCTAD', 'ice_unctad'): 'ICE_UNCTAD',
        # TURISMO PAISES AÑO CERRADO
        ('TURISMO CERRADO', 'PAIS_RESIDENCIA'): 'TURISMO_PAIS_CERRADO',
        # TURISMO DEPARTAMENTOS AÑO CERRADO
        ('TURISMO CERRADO', 'DPTO_HOSPEDAJE'): 'TURISMO_DPTO_CERRADO',
        # TURISMO CIUDAD AÑO CERRADO
        ('TURISMO CERRADO', 'CIUDAD_HOSPEDAJE'): 'TURISMO_MUN_CERRADO',
        # TURISMO GENERO AÑO CERRADO
        ('TURISMO CERRADO', 'DESCRIPCION_GENERO'): 'TURISMO_GEN_CERRADO',
        # TURISMO MOTIVO AÑO CERRADO
        ('TURISMO CERRADO', 'MOVC_NOMBRE'): 'TURISMO_MOV_CERRADO',
        # TURISMO PAISES AÑO CORRIDO
        ('TURISMO CORRIDO', 'PAIS_RESIDENCIA'): 'TURISMO_PAIS_CORRIDO',
        # TURISMO DEPARTAMENTOS AÑO CORRIDO
        ('TURISMO CORRIDO', 'DPTO_HOSPEDAJE'): 'TURISMO_DPTO_CORRIDO',
        # TURISMO CIUDAD AÑO CORRIDO
        ('TURISMO CORRIDO', 'CIUDAD_HOSPEDAJE'): 'TURISMO_MUN_CORRIDO',
        # TURISMO GENERO AÑO CORRIDO
        ('TURISMO CORRIDO', 'DESCRIPCION_GENERO'): 'TURISMO_GEN_CORRIDO',
        # TURISMO MOTIVO AÑO CORRIDO
        ('TURISMO CORRIDO', 'MOVC_NOMBRE'): 'TURISMO_MOV_CORRIDO',
        # EXPORTACIONES POR PESO POR MEDIO DE TRANSPORTE AÑO CERRADO
        ('MEDIOS PESO MINERO', 'ST_CATEGORIAS_PESO_CERRADO'): 'EXPO_MEDIOS_MINERO_PESO_CERRADO',
        ('MEDIOS PESO NO MINERO', 'ST_CATEGORIAS_PESO_CERRADO'): 'EXPO_MEDIOS_NME_PESO_CERRADO',
        # EXPORTACIONES POR PESO POR MEDIO DE TRANSPORTE AÑO CORRIDO
        ('MEDIOS PESO MINERO', 'ST_CATEGORIAS_PESO_CORRIDO'): 'EXPO_MEDIOS_MINERO_PESO_CORRIDO',
        ('MEDIOS PESO NO MINERO', 'ST_CATEGORIAS_PESO_CORRIDO'): 'EXPO_MEDIOS_NME_PESO_CORRIDO',
    }

    # Lista que define el orden en el que se guardarán las pestañas en el archivo Excel
    orden_deseado = [
        # VALORES TOTALES DE EXPORTACIONES
        ('TOTALES', 'ST_CATEGORIAS_CERRADO'),
        ('TOTALES', 'ST_CATEGORIAS_CORRIDO'),
        ('TIPOS', 'ST_CATEGORIAS_CERRADO'),
        ('TIPOS', 'ST_CATEGORIAS_CORRIDO'),
        ('TOTALES PESO', 'ST_CATEGORIAS_PESO_CERRADO'),
        ('TOTALES PESO', 'ST_CATEGORIAS_PESO_CORRIDO'),
        ('TIPOS PESO', 'ST_CATEGORIAS_PESO_CERRADO'),
        ('TIPOS PESO', 'ST_CATEGORIAS_PESO_CORRIDO'),
        ('MEDIOS PESO MINERO', 'ST_CATEGORIAS_PESO_CERRADO'),
        ('MEDIOS PESO NO MINERO', 'ST_CATEGORIAS_PESO_CERRADO'),
        ('MEDIOS PESO MINERO', 'ST_CATEGORIAS_PESO_CORRIDO'),
        ('MEDIOS PESO NO MINERO', 'ST_CATEGORIAS_PESO_CORRIDO'),
        # CATEGORÍAS CERRADO
        ('CATEGORIAS CERRADO', 'CONTINENTE'),
        ('CATEGORIAS CERRADO', 'DEPARTAMENTOS'),
        ('CATEGORIAS CERRADO', 'HUBS'),
        ('CATEGORIAS CERRADO', 'PAIS'),
        ('CATEGORIAS CERRADO', 'SECTORES'),
        ('CATEGORIAS CERRADO', 'SUBSECTORES'),
        ('CATEGORIAS CERRADO', 'TLCS'),
        # CATEGORÍAS CORRIDO
        ('CATEGORIAS CORRIDO', 'CONTINENTE'),
        ('CATEGORIAS CORRIDO', 'DEPARTAMENTOS'),
        ('CATEGORIAS CORRIDO', 'HUBS'),
        ('CATEGORIAS CORRIDO', 'PAIS'),
        ('CATEGORIAS CORRIDO', 'SECTORES'),
        ('CATEGORIAS CORRIDO', 'SUBSECTORES'),
        ('CATEGORIAS CORRIDO', 'TLCS'),
        # DATOS DE EMPRESAS
        ('EMPRESAS', 'ST_NIT_CERRADO'),
        ('EMPRESAS', 'ST_NIT_CORRIDO'),
        # CONTEO DE EMPRESAS
        ('CONTEO EMPRESAS', 'CERRADO'),
        ('CONTEO EMPRESAS', 'CORRIDO'),
        # BALANZA COMERCIAL
        ('BALANZA', 'BALANZA'),
        # IED TOTAL
        ('IED TOTAL', ''),
        # IED ACTIVIDADES
        ('IED ACTIVIDADES COLOMBIA', 'ied_cerrado'),
        ('IED ACTIVIDADES COLOMBIA', 'ied_corrido'),
        # IED PAISES
        ('IED PAISES', 'ied_cerrado'),
        ('IED PAISES', 'ied_corrido'),
        # ICE TOTAL
        ('ICE TOTAL', ''),
        ('ICE PAISES', 'ice_cerrado'),
        ('ICE PAISES', 'ice_corrido'),
        # IED UNCTAD
        ('UNCTAD', 'ied_unctad'),
        # ICE UNCTAD
        ('UNCTAD', 'ice_unctad'),
        # TURISMO CERRADO
        ('TURISMO CERRADO', 'PAIS_RESIDENCIA'),
        ('TURISMO CERRADO', 'DPTO_HOSPEDAJE'),
        ('TURISMO CERRADO', 'CIUDAD_HOSPEDAJE'),
        ('TURISMO CERRADO', 'DESCRIPCION_GENERO'),
        ('TURISMO CERRADO', 'MOVC_NOMBRE'),
        # TURISMO CORRIDO
        ('TURISMO CORRIDO', 'PAIS_RESIDENCIA'),
        ('TURISMO CORRIDO', 'DPTO_HOSPEDAJE'),
        ('TURISMO CORRIDO', 'CIUDAD_HOSPEDAJE'),
        ('TURISMO CORRIDO', 'DESCRIPCION_GENERO'),
        ('TURISMO CORRIDO', 'MOVC_NOMBRE')
    ]

    # Crear un objeto ExcelWriter para escribir múltiples DataFrames en un archivo Excel
    with pd.ExcelWriter(file_path, engine='xlsxwriter') as writer:
        # Iterar sobre el orden deseado de las llaves y subllaves
        for key, sub_key in orden_deseado:
            # Omitir ciertas claves específicas que no se desean guardar
            if key in ['RESUMEN', 'OPORTUNIDADES', 'CONECTIVIDAD']:
                continue
            # Verificar si la clave y subclave existen en data_dict
            if key in data_dict and sub_key in data_dict[key]:
                df = data_dict[key][sub_key]
                # Verificar si el objeto es un DataFrame
                if isinstance(df, pd.DataFrame):
                    # Asegurarse de que el DataFrame no esté vacío
                    if not df.empty:
                        # Buscar el nombre de la pestaña en el diccionario de mapeo
                        sheet_name = sheet_name_mapping.get((key, sub_key), f"{key}_{sub_key}")
                        # Limitar el nombre de la pestaña a 31 caracteres (límite de Excel)
                        sheet_name = sheet_name[:31]
                        # Guardar el DataFrame en la pestaña correspondiente
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                else:
                    # Si el objeto no es un DataFrame, asume que es un número u otro tipo escalar
                    # Crear un DataFrame con una sola celda para almacenar el valor
                    single_value_df = pd.DataFrame({'Valor': [df]})
                    # Buscar el nombre de la pestaña en el diccionario de mapeo
                    sheet_name = sheet_name_mapping.get((key, sub_key), f"{key}_{sub_key}")
                    # Limitar el nombre de la pestaña a 31 caracteres (límite de Excel)
                    sheet_name = sheet_name[:31]
                    # Guardar el DataFrame de un solo valor en la pestaña correspondiente
                    single_value_df.to_excel(writer, sheet_name=sheet_name, index=False)