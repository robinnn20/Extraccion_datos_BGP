import subprocess
import requests
import re
import os
import smtplib
from datetime import datetime
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

# Configuración del correo
SMTP_SERVER = "smtp.gmail.com"  # Servidor SMTP de Gmail
SMTP_PORT = 587 
SMTP_USER = "informacioon.semanal@gmail.com"  # Correo creado especificamente para enviar las metricas
SMTP_PASS = "qllz hwrw yxxc qmgp"  
DESTINATARIO = "robinhidalgo169@gmail.com"  # Destinatario del correo

# Ruta de archivos
#nombre de archivo que se utilizara para el archivo RIB mas reciente
SAVE_PATH = "rib_latest.bz2"
#nombre del archivo luego de aplicar el bgpdump
RIB_OUTPUT_FILE = "datos_rib.txt"
#nombre del rachivo luego de aplicar el filtro de columnas y quedar con solo 2 columnas
FILTERED_OUTPUT_FILE = "datos_columnas_filtradas.txt"
#nombre del archivo de los logs
LOG_PATH = "ejecucion.log"

# URL base de Route Views Chile
BASE_URL = "https://routeviews.org/route-views.chile/bgpdata/2025.02/RIBS/"

def obtener_archivo_rib_mas_reciente():
    response = requests.get(BASE_URL)
    if response.status_code != 200:
        log(" Error al acceder a la página de archivos RIB.")
        return None

    archivos_rib = re.findall(r'(rib\.\d{8}\.\d{4}\.bz2)', response.text)
    if not archivos_rib:
        log(" No se encontraron archivos RIB.")
        return None
    
    archivo_rib = sorted(archivos_rib, reverse=True)[0]
    
    return f"{BASE_URL}{archivo_rib}"


#funcion para descargar archivo rib
#toma como parametros la url y el nombre com osera guardado
def descargar_archivo(url, save_path):
    log(f"⬇ Descargando archivo desde {url} ...")
    #se hace la consulta para ver si se puede descargar  
    response = requests.get(url, stream=True)
    if response.status_code == 200:
        with open(save_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024):
                f.write(chunk)
        log(" Descarga completada.")
        return True
    else:
        log(" Error al descargar el archivo.")
        return False

def ejecutar_script(comando):
    try:
        resultado = subprocess.run(comando, text=True, capture_output=True, check=True)
        log(f" {comando[1]} ejecutado correctamente.")
        return resultado.stdout  # Devolvemos la salida del script
    except subprocess.CalledProcessError as e:
        log(f" Error ejecutando {comando[1]}: {e}")
        return None
#funcion para enviar correo
def enviar_correo(asunto, mensaje):
    try:
        msg = MIMEMultipart()
        msg["From"] = SMTP_USER
        msg["To"] = DESTINATARIO
        msg["Subject"] = asunto

        msg.attach(MIMEText(mensaje, "plain"))

        server = smtplib.SMTP(SMTP_SERVER, SMTP_PORT)
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, DESTINATARIO, msg.as_string())
        server.quit()

        log(" Correo enviado correctamente.")
    except Exception as e:
        log(f" Error al enviar el correo: {e}")


#funcion que encuentra y elimina los archivos segun sus nombres para evitar uso de espacio innecesario luego de terminar el proceso 
def limpiar_archivos():
    for archivo in [RIB_OUTPUT_FILE, FILTERED_OUTPUT_FILE, 'rib_latest.bz2', 'asn_cache_json']:
        if os.path.exists(archivo):
            os.remove(archivo)
            log(f"Archivo {archivo} eliminado.")

def log(mensaje):
    with open(LOG_PATH, "a") as f:
        f.write(f"{datetime.now()} - {mensaje}\n")

# Obtener la URL del último archivo RIB
url_rib = obtener_archivo_rib_mas_reciente()
if url_rib and descargar_archivo(url_rib, SAVE_PATH):
    # Ejecutar bgpdump
    log("Procesando con bgpdump...")
    result = subprocess.run(["bgpdump", "-m", SAVE_PATH, "-O", RIB_OUTPUT_FILE])

    if result.returncode == 0:  
        log(" bgpdump ejecutado correctamente.")
        
        # Ejecutar filtrar_columnas.py
        ejecutar_script(["python3", "filtrar_columnas.py"])
        
        salida_d = ejecutar_script(["python3", "cal_de_metricas.py"])
        
        if salida_d:
            # Enviar correo con las métricas
            semana_actual = datetime.now().isocalendar()[1]  # Obtener la semana del año
            asunto = f"Summary BGP IPv6"
            mensaje = f"Este correo es automático. Se adjunta la información de las métricas BGP IPv6 obtenidas esta semana:\n\n" \
          f"Analysis Summary\n" \
          f"----------------\n" \
          f"{salida_d}\n" \
          f"----------------"

            enviar_correo(asunto, mensaje)
        
        # Limpiar archivos temporales
        limpiar_archivos()
    else:
        log(" bgpdump falló.")
else:
    log(" No se pudo descargar el archivo RIB.")
