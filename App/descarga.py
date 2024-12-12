# Librerias
# Datos
import datos as dat
# Documentos 
import documentos as doc
# Conversión
import io
import base64
# Streamlit
import streamlit as st
# Pandas
import pandas as pd

# Función para cargar el archivo .xlsx con los correos de los usuarios autorizados
def load_authorized_users():
    # Leer archivo de planta
    df = pd.read_excel(".streamlit/Planta.xlsx")

    # Eliminar espacios
    df['CORREO'] = df['CORREO'].str.strip()

    # Extrar correos
    df_list = df['CORREO'].tolist()

    # Devolver lista
    return df_list

# Función para insertar datos en la tabla de seguimiento
def registrar_evento(sesion_activa, tipo_evento, detalle_evento, unidad, correo, tipo_boton):
    """
    Registra un evento en la base de datos Snowflake.

    Args:
    - sesion_activa: Sesión activa de conexión a la base de datos.
    - tipo_evento (str): Tipo de evento ('selección' o 'descarga').
    - detalle_evento (str): Detalle de evento ('selección continente', 'selección país', etc)
    - unidad (str): Unidad específica del evento (e.g., 'América', 'Colombia').
    - correo (str): Correo electrónico con que el usuario se validó o realizó la descarga.
    - tipo_boton (str): Tipo de botón con tres valores "Selección", "Validación de correo electrónico" o  "Descarga validada".
    """
    # Crear objeto de conexión
    conn = sesion_activa.connection
    try:
        # Crear consulta para el insert
        query_insert = f"""
        INSERT INTO DOCUMENTOS_COLOMBIA.SEGUIMIENTO.SEGUIMIENTO_EVENTOS (TIPO_EVENTO, DETALLE_EVENTO, UNIDAD, CORREO, TIPO_BOTON, FECHA_HORA) 
        VALUES ('{tipo_evento}', '{detalle_evento}', '{unidad}', '{correo}', '{tipo_boton}', CONVERT_TIMEZONE('America/Los_Angeles', 'America/Bogota', CURRENT_TIMESTAMP));
        """
        # Crear un cursor para ejecutar la consulta
        cur = conn.cursor()
        try:
            # Ejecutar la consulta SQL con los valores
            cur.execute(query_insert)
        finally:
            # Cerrar el cursor
            cur.close()
    # Error
    except Exception as e:
        st.write(f"Error al registrar evento: {e}")

    
# Función para generar archivos sin generar botón de descarga
@st.cache_data(show_spinner=False)
def generar_documentos(agrupacion, _sesion_activa, continentes=None, paises=None, hubs=None, tlcs=None, departamentos=None, umbral=[10000], header_image_left=None, footer_image=None):
    
    """
    Genera documentos Word y Excel para la agrupación seleccionada y los pone disponibles para descarga.

    Args:
    - agrupacion (str): Tipo de agrupación para el informe (e.g., 'CONTINENTES', 'PAISES', 'HUBS', 'TLCS', 'DEPARTAMENTOS', 'COLOMBIA').
    - _sesion_activa: Sesión activa de conexión a la base de datos (OBJETO NO HASHEABLE POR ELLO SE PONE _ AL INICIO).
    - continentes (tuple, optional): Tupla de continentes seleccionados. Default es None.
    - paises (tuple, optional): Tupla de países seleccionados. Default es None.
    - hubs (tuple, optional): Tupla de HUBs seleccionados. Default es None.
    - tlcs (tuple, optional): Tupla de TLCs seleccionados. Default es None.
    - departamentos (tuple, optional): Tupla de departamentos seleccionados. Default es None.
    - umbral (tuple, optional): Tupla de umbrales de valores. Default es (10000,).
    - header_image_left (str, optional): Ruta a la imagen del encabezado izquierdo. Default es None.
    - header_image_right (str, optional): Ruta a la imagen del encabezado derecho. Default es None.
    - footer_image (str, optional): Ruta a la imagen del pie de página. Default es None.
    """

    # Convertir tuplas a listas, o definir como None si no se proporcionan valores
    continentes = list(continentes) if continentes else None
    paises = list(paises) if paises else None
    hubs = list(hubs) if hubs else None
    tlcs = list(tlcs) if tlcs else None
    departamentos = list(departamentos) if departamentos else None
    umbral = list(umbral) if umbral else None

    # Mostrar barra de progreso y spinner
    progress_bar = st.progress(0)
    with st.spinner('Generando el documento, por favor espere...'):
        try:
            # Obtener parámetros de datos
            geo_params = dat.get_data_parametros(_sesion_activa, agrupacion, continentes, paises, hubs, tlcs, departamentos, umbral)
            # Obtener diccionario de verificación de datos 
            dict_verificacion = dat.verif_ejes(_sesion_activa, geo_params)
            # Actualizar progreso
            progress_bar.progress(5)
            
            # Procesar datos
            tables, tables_excel = dat.process_data(_sesion_activa, geo_params, dict_verificacion)
            progress_bar.progress(50)

            # Determinar los nombres de los archivos
            if agrupacion == 'COLOMBIA':
                file_name_suffix = 'Colombia'
            else:
                entity_name = (continentes[0] if continentes else
                               paises[0] if paises else
                               hubs[0] if hubs else
                               tlcs[0] if tlcs else
                               departamentos[0])
                file_name_suffix = f"{agrupacion} - {entity_name}"

            # Nombres de archivo para descarga
            file_name_docx = f"Tres Ejes {file_name_suffix}.docx"
            file_name_xlsx = f"Tres Ejes {file_name_suffix}.xlsx"

            # Crear objetos BytesIO
            docx_buffer = io.BytesIO()
            xlsx_buffer = io.BytesIO()

            # Generar el documento Word y registrar evento de selección en la base de datos
            if agrupacion == 'CONTINENTES':
                doc.create_document_continentes(tablas=tables, file_path=docx_buffer, titulo=continentes[0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params, dict_verificacion=dict_verificacion)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de continente', unidad=continentes[0], correo='Correo sin validar', tipo_boton='Selección')
            elif agrupacion == 'PAISES':
                doc.create_document_paises(tablas=tables, file_path=docx_buffer, titulo=geo_params['NOMBRE PAIS'][0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params, dict_verificacion=dict_verificacion)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de país', unidad=paises[0], correo='Correo sin validar', tipo_boton='Selección')
            elif agrupacion == 'HUBS':
                doc.create_document_hubs(tablas=tables, file_path=docx_buffer, titulo=hubs[0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params, dict_verificacion=dict_verificacion)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de HUB', unidad=hubs[0], correo='Correo sin validar', tipo_boton='Selección')   
            elif agrupacion == 'TLCS':
                doc.create_document_tlcs(tablas=tables, file_path=docx_buffer, titulo=tlcs[0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params, dict_verificacion=dict_verificacion)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de TLC', unidad=tlcs[0], correo='Correo sin validar', tipo_boton='Selección')
            elif agrupacion == 'DEPARTAMENTOS':
                doc.create_document_departamentos(tablas=tables, file_path=docx_buffer, titulo=departamentos[0], header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, geo_params=geo_params, dict_verificacion=dict_verificacion)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de departamento', unidad=departamentos[0], correo='Correo sin validar', tipo_boton='Selección')
            elif agrupacion == 'COLOMBIA':
                doc.create_document_colombia(tablas=tables, file_path=docx_buffer, header_image_left=header_image_left, footer_image=footer_image, session=_sesion_activa, dict_verificacion=dict_verificacion)
                registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Selección', detalle_evento='Selección de Colombia', unidad='Colombia', correo='Correo sin validar', tipo_boton='Selección')
            else:
                raise ValueError("Agrupación no reconocida")
            # Actualizar estado
            progress_bar.progress(60)
        
            # Crear el archivo Excel utilizando la función original
            dat.guardar_tablas_en_excel(data_dict=tables_excel, file_path=xlsx_buffer)
            progress_bar.progress(75)

            # Mover los punteros al inicio
            docx_buffer.seek(0)
            xlsx_buffer.seek(0)

            # Codificar los archivos en base64
            b64_docx = base64.b64encode(docx_buffer.read()).decode()
            b64_xlsx = base64.b64encode(xlsx_buffer.read()).decode()

            # Almacenar en session_state
            st.session_state['b64_docx'] = b64_docx
            st.session_state['b64_xlsx'] = b64_xlsx
            st.session_state['file_name_docx'] = file_name_docx
            st.session_state['file_name_xlsx'] = file_name_xlsx

            # Actualizar progreso al 100%
            progress_bar.progress(100)
            
            # Mostrar mensaje de éxito cuando los documentos están listos
            st.success("El documento ha sido generado exitosamente. Puede descargarlo a continuación:")
            
        except Exception as e:
            # Mostrar mensaje de error en caso de excepción
            st.error(f"Se produjo un error durante la generación del documento: {e}")
        finally:
            # Finalizar barra de progreso
            progress_bar.empty()

# Función para crear los botones de descarga con validación de correo
def botones_descarga_word_xlsx(agrupacion, _sesion_activa, unidad):

    """
    Genera botones de descarga para documentos en formatos Word y Excel, con eventos de registro.
    Implementa un proceso donde se solicita el correo electrónico antes de permitir la descarga.

    Args:
    - agrupacion (str): Tipo de agrupación para el informe (e.g., 'CONTINENTES', 'PAISES', 'HUBS', 'TLCS', 'DEPARTAMENTOS', 'COLOMBIA').
    - _sesion_activa: Sesión activa de conexión a la base de datos.
    - unidad (tuple or list): Unidad seleccionada para el evento (e.g., continente, país, HUB).
    """

    # Cargar lista de usuarios verificados
    usuarios_verificados =  load_authorized_users()

    # Verificar que los archivos están en session_state
    if 'b64_docx' not in st.session_state or 'b64_xlsx' not in st.session_state:
        st.error("No se encontraron los documentos para descargar. Por favor, genere el documento nuevamente.")
        return

    b64_docx = st.session_state['b64_docx']
    b64_xlsx = st.session_state['b64_xlsx']
    file_name_docx = st.session_state['file_name_docx']
    file_name_xlsx = st.session_state['file_name_xlsx']

    # Creación de detalles de eventos:
    # Parte común 
    descripcion_evento_word = 'Descarga Word de '
    descripcion_evento_excel = 'Descarga Excel de '
    # Agregar final según agrupación
    if agrupacion == 'CONTINENTES':
        descripcion_evento_word += 'continente'
        descripcion_evento_excel += 'continente'
    elif agrupacion == 'PAISES':
        descripcion_evento_word += 'país'
        descripcion_evento_excel += 'país'
    elif agrupacion == 'HUBS':
        descripcion_evento_word += 'HUB'
        descripcion_evento_excel += 'HUB'
    elif agrupacion == 'TLCS':
        descripcion_evento_word += 'TLC'
        descripcion_evento_excel += 'TLC'
    elif agrupacion == 'DEPARTAMENTOS':
        descripcion_evento_word += 'departamento'
        descripcion_evento_excel += 'departamento'
    elif agrupacion == 'COLOMBIA':
        descripcion_evento_word += 'Colombia'
        descripcion_evento_excel += 'Colombia'
    else:
        raise ValueError("Agrupación no reconocida")
    
    # Convertir unidad a cadena
    unidad_evento = str(unidad[0]) if isinstance(unidad, (tuple, list)) else str(unidad) if unidad else None

    # Gestión de estado para Word
    if 'word_download_clicked' not in st.session_state:
        st.session_state['word_download_clicked'] = False
    if 'word_email_validated' not in st.session_state:
        st.session_state['word_email_validated'] = False
    if 'email_word' not in st.session_state:
        st.session_state['email_word'] = ''

    # Gestión de estado para Excel
    if 'excel_download_clicked' not in st.session_state:
        st.session_state['excel_download_clicked'] = False
    if 'excel_email_validated' not in st.session_state:
        st.session_state['excel_email_validated'] = False
    if 'email_excel' not in st.session_state:
        st.session_state['email_excel'] = ''

    # WORD Download Process
    if not st.session_state['word_download_clicked']:
        if st.button('Descargar el documento en Microsoft Word', type='secondary', use_container_width=True):
            st.session_state['word_download_clicked'] = True
    if st.session_state['word_download_clicked'] and not st.session_state['word_email_validated']:
        # Solicitar correo electrónico
        email_word = st.text_input('Por favor, introduzca su correo electrónico de ProColombia', key='email_word')
        if email_word:
            # Registrar evento de validación
            registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Descarga', detalle_evento=descripcion_evento_word, unidad=unidad_evento, correo=email_word, tipo_boton='Validación de correo electrónico')
            # Validar el correo
            if email_word in usuarios_verificados:
                st.session_state['word_email_validated'] = True
                st.session_state['word_email'] = email_word
                st.success('Correo electrónico validado. Puede descargar el documento.')
            else:
                st.error('El correo electrónico debe ser un correo válido para funcionarios de ProColombia')
    # Si el correo es válido, mostrar botón de descarga
    if st.session_state['word_email_validated']:
        st.download_button(label='Descargar el documento en Microsoft Word', data=base64.b64decode(b64_docx), 
                file_name=file_name_docx, help='Presione el botón para descargar el archivo Word', 
                mime='application/vnd.openxmlformats-officedocument.wordprocessingml.document', 
                on_click=lambda: registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Descarga', detalle_evento=descripcion_evento_word, unidad=unidad_evento, correo=st.session_state['word_email'], tipo_boton='Descarga validada'),
                type='secondary',
                use_container_width=True)
        
    # Excel Download Process
    if not st.session_state['excel_download_clicked']:
        if st.button('Descargar el documento en Microsoft Excel', type='secondary', use_container_width=True):
            st.session_state['excel_download_clicked'] = True
    if st.session_state['excel_download_clicked'] and not st.session_state['excel_email_validated']:
        # Solicitar correo electrónico
        email_excel = st.text_input('Por favor, introduzca su correo electrónico de ProColombia', key='email_excel')
        if email_excel:
            # Registrar evento de validación
            registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Descarga', detalle_evento=descripcion_evento_excel, unidad=unidad_evento, correo=email_excel, tipo_boton='Validación de correo electrónico')
            # Validar el correo
            if email_excel in usuarios_verificados:
                st.session_state['excel_email_validated'] = True
                st.session_state['excel_email'] = email_excel
                st.success('Correo electrónico validado. Puede descargar el documento.')
            else:
                st.error('El correo electrónico debe ser un correo válido para funcionarios de ProColombia')
    # Si el correo es válido, mostrar botón de descarga
    if st.session_state['excel_email_validated']:
        st.download_button(label='Descargar el documento en Microsoft Excel', data=base64.b64decode(b64_xlsx), 
                file_name=file_name_xlsx, help='Presione el botón para descargar el archivo Excel', 
                mime='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet', 
                on_click=lambda: registrar_evento(sesion_activa=_sesion_activa, tipo_evento='Descarga', detalle_evento=descripcion_evento_excel, unidad=unidad_evento, correo=st.session_state['excel_email'], tipo_boton='Descarga validada'),
                type='secondary',
                use_container_width=True)
