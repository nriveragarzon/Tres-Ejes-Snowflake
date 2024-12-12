#######################
# 0. Importar librerias
#######################
import warnings
import streamlit as st
import selectores as selectores
import descarga as desc
import time
from datetime import datetime, timedelta

warnings.filterwarnings("ignore", message="Bad owner or permissions on")

# Configuración página web
st.set_page_config(page_title="Documentos Tres Ejes", page_icon = ':bar_chart:', layout="wide",  initial_sidebar_state="expanded")

# Imágenes
# Página de inicio
inicio_img = 'Insumos/banner_inicio.png'

# Página de documentos
documentos_img = 'Insumos/banner_documentos.png'

# Página de fuente
fuente_img = 'Insumos/banner_fuentes.png'

# Footer para todas las páginas
footer_img = 'Insumos/banner_footer.png'

# Logos Procolombia
logo_img = 'Insumos/logos - Comercio - ProColombia_COLOR.png'

# Documento
top_left_img = 'Insumos/doc_top_left.png'
bottom_right = 'Insumos/doc_bottom_right.png'

# Secrets
def cargar_contraseñas(nombre_archivo):
    """
    Función para cargar contraseñas
    """
    return st.secrets

# Cargar contraseñas
cargar_contraseñas(".streamlit/secrets.toml")

# Inicializar variables de sesión si no existen
if 'session' not in st.session_state:
    st.session_state.session = None  # Sesión inicializada como None
if 'last_activity_time' not in st.session_state:
    st.session_state.last_activity_time = datetime.now()  # Última actividad es el momento actual

# Definir tiempo de espera de sesión (15 minutos)
SESSION_TIMEOUT = timedelta(minutes=15)

# Función para crear una nueva sesión con Snowflake
def create_session(retries=3, wait=5):
    """
    Crea una nueva sesión de Snowflake, con lógica de reintentos en caso de fallo.
    
    Args:
        retries (int): Número máximo de intentos de conexión. Por defecto, 3.
        wait (int): Tiempo en segundos entre intentos. Por defecto, 5.

    Returns:
        session: Objeto de sesión activa de Snowflake o None si todos los intentos fallan.
    """
    success = False
    for attempt in range(retries):
        try:
            # Conectar a Snowflake
            connection = st.connection("snowflake") # Establecer conexión con Snowflake
            sesion_activa = connection.session()  # Obtener sesión acti
            success = True
            break
        except Exception as e:
            print(f"Intento {attempt + 1} de {retries} fallido: \n{str(e)}")
            if attempt < retries - 1:
                time.sleep(wait) # Esperar antes del siguiente intento
    if success:
        return sesion_activa # Return el objeto de sesión de Snowflake
    else:
        print("Todos los intentos de conexión fallaron.")
        return None

# Función para verificar si la sesión ha expirado
def check_session():
    """
    Verifica si la sesión actual ha expirado. Si es así, cierra la sesión.
    """
    if (datetime.now() - st.session_state.last_activity_time) > SESSION_TIMEOUT:
        if st.session_state.session:
            st.session_state.session.close()  # Cerrar la sesión expirada
        st.session_state.session = None

# Función para obtener la sesión activa
def get_session():
    """
    Obtiene la sesión activa de Snowflake. Si no existe o ha expirado, crea una nueva.
    """
    check_session()  # Verificar si la sesión ha expirado
    if st.session_state.session is None:
        st.session_state.session = create_session()  # Crear nueva sesión si no existe

# Función para actualizar el tiempo de última actividad
def update_last_activity():
    """
    Actualiza el tiempo de última actividad de la sesión al momento actual.
    """
    st.session_state.last_activity_time = datetime.now()  # Registrar última actividad

def flujo_snowflake():
    """
    Gestiona el flujo para interactuar con Snowflake, asegurando que:
    1. Se registre la última actividad del usuario.
    2. Se obtenga una sesión activa, ya sea verificando la existente o creando una nueva.

    Este flujo utiliza funciones auxiliares para manejar la sesión y garantizar
    que el tiempo de espera (timeout) y la lógica de reconexión se respeten.
    """
    # Paso 1: Actualizar el tiempo de última actividad
    # Esto asegura que el registro de actividad esté actualizado para prevenir 
    # el cierre prematuro de la sesión por inactividad.
    update_last_activity()

    # Paso 2: Obtener la sesión activa
    # Verifica si hay una sesión activa. Si ha expirado o no existe, 
    # intenta crear una nueva sesión de Snowflake.
    get_session()

# Limpiar cache
def limpiar_cache():
    """
    Limpia el cache de datos de Streamlit.
    """
    st.cache_data.clear()  # Limpia el cache de datos de Streamlit

######################################
# 1. Definir el flujo de la aplicación
######################################

def main():
    
    # Flujo de Snowflake:
    flujo_snowflake()

    ## Menú de navegación
    ### Logo ProColombia
    with st.sidebar:
        st.image(logo_img, caption=None, use_column_width="always")
        st.markdown('#')     
    ## Páginas
    ### Opciones con iconos
    options = {
        "Portada": "Portada",
        "Documentos": "Documentos",
        "Fuentes": "Fuentes"
    }
    #### Configuración del sidebar
    page = st.sidebar.radio("Elija una página", list(options.keys()))
    selected_option = options.get(str(page))  # Usar get para manejar None de manera segura
    if selected_option:
        if selected_option == "Portada":
            page_portada()
        elif selected_option == "Documentos":
            documentos()
        elif selected_option == "Fuentes":
            page_fuentes()
    ### Logo MINCIT
    with st.sidebar:
        ### Elaborado por la Coordinación de Analítica
        st.markdown('#')
        st.subheader("Elaborado por:") 
        st.write("Coordinación de Analítica, Gerencia de Inteligencia Comercial, ProColombia.") 
        st.markdown('#')
        
        
###########################
# Personalización de estilo
###########################

# Función para cargar el CSS desde un archivo
def load_css(file_name):
    with open(file_name) as f:
        return f.read()

# Cargar y aplicar el CSS personalizado
css = load_css("styles.css")
st.markdown(f"<style>{css}</style>", unsafe_allow_html=True)

##############
# Contenido
##############

#########
# Portada
#########

def page_portada():

    # Actualizar tiempo de última actividad
    update_last_activity()

    # Banner
    st.image(image=inicio_img, caption=None, use_column_width="always")

    # Contenido de la portada
    st.markdown("""
    <style>
    .justify-text {
        text-align: justify;
    }
    .justify-text p, .justify-text div {
        margin-bottom: 10px; /* Espacio entre los párrafos */
    }
    .justify-text .indent {
        margin-left: 20px; /* Ajusta este valor para cambiar la indentación del texto */
    }
    </style>
    </div>
    <h2>Soporte</h2>
    <div class="justify-text">
        <p>Si tiene alguna pregunta o necesita asistencia, no dude en ponerse en contacto con el equipo de la Coordinación de Analítica, Gerencia de Inteligencia Comercial, ProColombia.</p>
    </div>               
    """, unsafe_allow_html=True)
    # Footer
    st.image(image=footer_img, caption=None, use_column_width="always")

############
# Documentos
############

def documentos():

    # Flujo de Snowflake:
    flujo_snowflake()

    # Banner
    st.image(image=documentos_img, caption=None, use_column_width="always")
    # Instrucciones de descarga
    st.markdown("""
    <style>
    .justify-text {
        text-align: justify;
    }
    .justify-text p, .justify-text div {
        margin-bottom: 10px; /* Espacio entre los párrafos */
    }
    .justify-text .indent {
        margin-left: 20px; /* Ajusta este valor para cambiar la indentación del texto */
    }
    </style>
    <h2>Empiece aquí</h2>
    <div class="justify-text">
    <p>Elija entre las siguientes opciones para empezar el proceso.</p>
    </div>               
    """, unsafe_allow_html=True)
    # Elección del usuario entre diferentes agrupaciones de datos
    eleccion_usuario = st.radio("Seleccione una opción", 
                                # Opciones: 
                                ('**Continente:** Explore un informe organizado por continente a nivel mundial.', 
                                 '**HUB:** Explore un informe organizado por HUB.',
                                 '**TLC:** Explore un informe organizado por Tratado de Libre Comercio.',
                                 '**País:** Explore un informe organizado por país.',
                                  '**Colombia:** Explore un informe organizado de Colombia.',
                                 '**Departamento:** Explore un informe organizado por departamento.'),
                                # Aclaración
                                help = "Seleccione una de las opciones para mostrar el contenido relacionado.",
                                on_change=limpiar_cache)
    
    # Actualizar tiempo de última actividad
    update_last_activity()

    # Continente
    if eleccion_usuario == "**Continente:** Explore un informe organizado por continente a nivel mundial.":
        
        # Flujo de Snowflake:
        flujo_snowflake()

        st.markdown("""
        <div class="justify-text">
        <p>Elija un continente para descargar el informe de interés.</p>
        </div>               
        """, unsafe_allow_html=True)
        continente_elegido = st.selectbox('Seleccione un continente:', selectores.selector_continentes(st.session_state.session), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el continente para descargar el informe de interés. Seleccione un único continente para refinar su búsqueda.', key = 'widget_continentes')
        
        # Se activa el proceso solo si el usuario elige una opción
        if continente_elegido:

            # Flujo de Snowflake:
            flujo_snowflake()

            # Generar los documentos, registrar el evento de selección y obtener los resultados
            continente_elegido_tuple = tuple([continente_elegido])
            desc.generar_documentos(
                agrupacion='CONTINENTES',
                _sesion_activa=st.session_state.session,
                continentes=continente_elegido_tuple,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            desc.botones_descarga_word_xlsx('CONTINENTES', st.session_state.session, continente_elegido)
                                
   # HUB
    if eleccion_usuario == "**HUB:** Explore un informe organizado por HUB.":

        # Flujo de Snowflake:
        flujo_snowflake()

        st.markdown("""
        <div class="justify-text">
        <p>Elija un HUB para descargar el informe de interés.</p>
        </div>               
        """, unsafe_allow_html=True)
        hub_elegido = st.selectbox('Seleccione un HUB:', selectores.selector_hubs(st.session_state.session), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el HUB para descargar el informe de interés. Seleccione un único HUB para refinar su búsqueda.', key = 'widget_hubs')
        
        # Se activa el proceso solo si el usuario elige una opción
        if hub_elegido:
            
            # Flujo de Snowflake:
            flujo_snowflake()

            # Generar los documentos, registrar el evento de selección y obtener los resultados
            hub_elegido_tuple = tuple([hub_elegido])
            desc.generar_documentos(
                agrupacion='HUBS',
                _sesion_activa=st.session_state.session,
                hubs=hub_elegido_tuple,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            desc.botones_descarga_word_xlsx('HUBS', st.session_state.session, hub_elegido)
            
    # TLCS
    if eleccion_usuario == '**TLC:** Explore un informe organizado por Tratado de Libre Comercio.':
        
        # Flujo de Snowflake:
        flujo_snowflake()

        st.markdown("""
        <div class="justify-text">
        <p>Elija un TLC para descargar el informe de interés.</p>
        </div>               
        """, unsafe_allow_html=True)
        tlc_elegido = st.selectbox('Seleccione un TLC:', selectores.selector_tlcs(st.session_state.session), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el TLC para descargar el informe de interés. Seleccione un único TLC para refinar su búsqueda.', key = 'widget_tlcs')
        
        # Se activa el proceso solo si el usuario elige una opción
        if tlc_elegido:

            # Flujo de Snowflake:
            flujo_snowflake()

            # Generar los documentos, registrar el evento de selección y obtener los resultados
            tlc_elegido_tuple = tuple([tlc_elegido])
            desc.generar_documentos(
                agrupacion='TLCS',
                _sesion_activa=st.session_state.session,
                tlcs=tlc_elegido_tuple,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            desc.botones_descarga_word_xlsx('TLCS', st.session_state.session, tlc_elegido)

    # País
    if eleccion_usuario == "**País:** Explore un informe organizado por país.":

        # Flujo de Snowflake:
        flujo_snowflake()

        st.markdown("""
        <div class="justify-text">
        <p>Elija un continente y luego un país para descargar el informe de interés.</p>
        </div>               
        """, unsafe_allow_html=True)
        continente_pais = st.selectbox('Seleccione un continente:', selectores.selector_continentes_paises(st.session_state.session), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el continente para descargar el informe de interés. Seleccione un único continente para refinar su búsqueda.', key = 'widget_continentes_pais')
        
        if continente_pais:
            pais_elegido = st.selectbox('Seleccione un país:', selectores.selector_paises(st.session_state.session, continente_pais), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el país para descargar el informe de interés. Seleccione un único país para refinar su búsqueda.', key = 'widget_pais')        
            
            # Flujo de Snowflake:
            flujo_snowflake()

            # Se activa el proceso solo si el usuario elige una opción
            if pais_elegido:

                # Flujo de Snowflake:
                flujo_snowflake()

                # Generar los documentos, registrar el evento de selección y obtener los resultados
                pais_elegido_tuple = tuple([pais_elegido])
                desc.generar_documentos(
                    agrupacion='PAISES',
                    _sesion_activa=st.session_state.session,
                    paises=pais_elegido_tuple,
                    header_image_left=top_left_img,
                    footer_image=bottom_right)
                # Botones de descarga
                desc.botones_descarga_word_xlsx('PAISES', st.session_state.session, pais_elegido)
                    
    # Colombia 
    if eleccion_usuario =="**Colombia:** Explore un informe organizado de Colombia.":
            
            # Flujo de Snowflake:
            flujo_snowflake()

            # Generar los documentos, registrar el evento de selección y obtener los resultados
            desc.generar_documentos(
                agrupacion='COLOMBIA',
                _sesion_activa=st.session_state.session,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            desc.botones_descarga_word_xlsx('COLOMBIA', st.session_state.session, 'Colombia')

    # Departamento
    if eleccion_usuario == "**Departamento:** Explore un informe organizado por departamento.":
        
        # Flujo de Snowflake:
        flujo_snowflake()

        st.markdown("""
        <div class="justify-text">
        <p>Elija un departamento para descargar el informe de interés.</p>
        </di
        v>               
        """, unsafe_allow_html=True)
        departamento_elegido = st.selectbox('Seleccione un departamento:', selectores.selector_departamento(st.session_state.session), index=None, placeholder='Elija una opción', help = 'Aquí puede elegir el departamento para descargar el informe de interés. Seleccione un único departamento para refinar su búsqueda.', key = 'widget_departamentos')
        
        # Se activa el proceso solo si el usuario elige una opción
        if departamento_elegido:
            
            # Flujo de Snowflake:
            flujo_snowflake()

            # Generar los documentos, registrar el evento de selección y obtener los resultados
            departamento_elegido_tuple = tuple([departamento_elegido])
            desc.generar_documentos(
                agrupacion='DEPARTAMENTOS',
                _sesion_activa=st.session_state.session,
                departamentos=departamento_elegido_tuple,
                header_image_left=top_left_img,
                footer_image=bottom_right)
            # Botones de descarga
            desc.botones_descarga_word_xlsx('DEPARTAMENTOS', st.session_state.session, departamento_elegido)

    # Footer
    st.image(image=footer_img, caption=None, use_column_width="always")


#########
# Fuentes
#########

def page_fuentes():
        
    # Actualizar tiempo de última actividad
    update_last_activity()

    # Banner
    st.image(image=fuente_img, caption=None, use_column_width="always")
    # Contenido de fuentes
    st.markdown("""
    <style>
    .justify-text {
        text-align: justify;
    }
    .justify-text p {
        margin-bottom: 10px; /* Espacio entre los párrafos */
    }
    .justify-text .indent {
        margin-left: 20px; /* Ajusta este valor para cambiar la indentación del texto */
    }            
    </style>
    <div class="justify-text">
    <h2>1. Exportaciones</h2>
    <ul class="indent">              
        <li><strong>Fuente:</strong> DANE - DIAN - Cálculos ProColombia.</li>
        <li><strong>Periodicidad de la información:</strong> Mensual con dos meses de rezago.</li>
        <li><strong>Descripción:</strong> La información sobre comercio exterior es generada a partir de los registros administrativos de importaciones y exportaciones (declaraciones de importación y exportación). Estos datos son validados, procesados y analizados conforme a las metodologías de la Organización de las Naciones Unidas (ONU), la Comunidad Andina de Naciones (CAN), y los criterios de calidad definidos por el Departamento Administrativo Nacional de Estadística (DANE).</li>
    </ul>
    <h2>2. Inversión</h2>
    <ul class="indent">                
        <li><strong>Fuente:</strong> Banco de la República - Cálculos ProColombia.</li>
        <li><strong>Periodicidad de la información:</strong> Trimestral con tres meses de rezago.</li>
        <li><strong>Descripción:</strong> La inversión directa representa los aportes de capital en los que existe una relación accionaria entre el inversionista y la empresa que reside en una economía diferente. Esta categoría, parte de la Balanza de Pagos, incluye:
            <ul class="indent">
                <li><strong>Inversión Extranjera Directa en Colombia (IED):</strong> Inversiones realizadas por residentes extranjeros en empresas colombianas (inversión directa pasiva).</li>
                <li><strong>Inversión Directa de Colombia en el Exterior (IDCE):</strong> Inversiones realizadas por colombianos en empresas en el exterior (inversión directa activa).</li>
            </ul>
        </li>
    </ul>
    <h2>3. Turismo</h2>
    <ul class="indent">
        <li><strong>Fuente:</strong> Migración Colombia - Cálculos ProColombia.</li>
        <li><strong>Periodicidad de la información:</strong> Mensual con dos meses de rezago.</li>
        <li><strong>Descripción:</strong> Estadísticas sobre la llegada de extranjeros no residentes a Colombia, clasificadas por país de residencia, departamento y ciudad de hospedaje. Se excluyen los residentes venezolanos y los colombianos residentes en el exterior, así como los cruceristas.</li>
    </ul>
    <h2>4. Conectividad Aérea</h2>
    <ul class="indent">
        <li><strong>Fuente:</strong> OAG - Cálculos ProColombia.</li>
        <li><strong>Periodicidad de la información:</strong> Mensual.</li>
        <li><strong>Descripción:</strong> Información detallada sobre vuelos nacionales, incluyendo aerolíneas, ciudades y departamentos de origen y destino, además de las frecuencias de los vuelos. Los datos también incluyen análisis de regiones y semanas específicas.</li>
    </ul>
    </div>
    """, unsafe_allow_html=True)

    # Footer
    st.image(image=footer_img, caption=None, use_column_width="always")



########################################
# Mostrar contenido de todas las páginas
########################################
if __name__ == "__main__":
    main()